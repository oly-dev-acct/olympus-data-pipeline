import os
import re
import smtplib
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from email.message import EmailMessage


# =====================================================
# CONFIGURATION: Folder Path → Required Groups Mapping
# FIX: Updated group names to match actual filenames in folder
# =====================================================
file_details = {
    r"Z:\BI_Reports\Home Office Groups - BI Reports": [
        'All Units - Excel',
        'Availability',
        'Contact Level Details',
        'Lease Details',
        'Lease Expiration Detail',               # FIX: was 'Lease Expiration Renewal Detail - Excel Report'
        'Leasing Activity Detail - Excel',
        'Resident Demographics',
        'Unit Scheduled Transactions',
        'Unit Setup View',
        'Unit turnover'
    ],
    r"Z:\BI_Reports\BI_Delinquent_Prepaid": [
        'Delinquent and Prepaid - Excel'
    ],
    r"Z:\BI_Reports\MaintenanceSummary": ['Service Request Activity']
}


# =====================================================
# CLASS
# =====================================================
class PropertyFileReconciliation:

    COMBINED_PROPERTY_MAP = {
        "Cedar Park and Canyon Falls Townhomes": ["Cedar Park", "Canyon Falls"],
        "Olympus Encantada": ["Olympus Encantada - San Pedro", "Olympus Encantada - Santa Monica"],
        "Olympus Alameda": ["Olympus Alameda - Phase I", "Olympus Alameda - Phase II"]
    }

    def __init__(self, db_config, folder_path, smtp_server, smtp_port,
                 sender_email, sender_password, recipients, file_details):

        self.db_config = db_config
        self.folder_path = folder_path
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password
        self.recipients = recipients
        self.file_details = file_details

        self.required_groups = self.file_details.get(self.folder_path, [])

        if not self.required_groups:
            raise ValueError(f"No group configuration found for path: {self.folder_path}")

        self.df_db = None
        self.df_files = None
        self.df_missing_files = None


    # -----------------------
    # Normalize Utility
    # -----------------------
    @staticmethod
    def normalize(text):
        return re.sub(r"\s+", "", str(text).lower().strip())

    @staticmethod
    def extract_dates_from_text(text):
        date_patterns = [
            r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',
            r'\b\d{4}-\d{1,2}-\d{1,2}\b',
            r'\b\d{1,2}-\d{1,2}-\d{2,4}\b'
        ]
        dates_found = []
        for pattern in date_patterns:
            matches = re.findall(pattern, str(text))
            dates_found.extend(matches)
        return dates_found


    # =================================================
    # SERVICE REQUEST ACTIVITY VALIDATION
    # =================================================
    def validate_service_request_activity(self, filepath):

        today = datetime.today().date()
        engine = "xlrd" if filepath.lower().endswith(".xls") else "openpyxl"

        try:
            df = pd.read_excel(filepath, nrows=20, engine=engine, header=None)
        except Exception as e:
            print(f"⚠️ Error reading {filepath}: {e}")
            return False

        range_valid = False
        today_found = False
        pattern = r'created\s*date\s*from\s*(\d{1,2}/\d{1,2}/\d{2,4})\s*to\s*(\d{1,2}/\d{1,2}/\d{2,4})'

        for val in df.values.flatten():
            if pd.isna(val):
                continue
            text = str(val)
            match = re.search(pattern, text.lower())
            if match:
                start_date = pd.to_datetime(match.group(1)).date()
                end_date   = pd.to_datetime(match.group(2)).date()
                print(f"🔎 Created Date Range Found: {start_date} → {end_date}")
                if start_date <= today <= end_date:
                    range_valid = True
                    print("✅ Today's date is inside Created Date range")

            for d in self.extract_dates_from_text(text):
                try:
                    if pd.to_datetime(d).date() == today:
                        today_found = True
                        print("✅ Today's date found in report")
                except:
                    continue

        if range_valid and today_found:
            print("✅ Service Request Activity file VALID")
            return True

        print("❌ Service Request Activity validation FAILED")
        return False


    # =================================================
    # FILE DATE VALIDATION
    # FIX: Added .csv to scanned extensions
    # =================================================
    def validate_today_file(self):

        today     = datetime.today().date()
        yesterday = today - timedelta(days=1)

        # FIX: include .csv files as well
        files = [
            entry for entry in os.scandir(self.folder_path)
            if entry.is_file()
            and entry.name.lower().endswith((".xlsx", ".xls", ".csv"))
            and "zhistorical" not in entry.name.lower()
        ]

        if not files:
            print(f"❌ No files found in {self.folder_path}")
            return False

        files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

        for entry in files:
            file_mdate = datetime.fromtimestamp(entry.stat().st_mtime).date()

            if file_mdate != today:
                print(f"⏭ Skipping {entry.name} (Modified: {file_mdate})")
                continue

            print(f"🔎 Checking file: {entry.name}")
            filepath = entry.path

            # Service Request special validation
            if "service request activity" in entry.name.lower():
                if self.validate_service_request_activity(filepath):
                    return True
                else:
                    continue

            # Skip CSV for content date check — modification date is sufficient
            if entry.name.lower().endswith(".csv"):
                print(f"✅ Valid CSV file found (today's date): {entry.name}")
                return True

            engine = "xlrd" if entry.name.lower().endswith(".xls") else "openpyxl"

            try:
                df = pd.read_excel(filepath, nrows=20, engine=engine, header=None)
            except Exception as e:
                print(f"⚠️ Error reading {entry.name}: {e}")
                continue

            for val in df.values.flatten():
                if pd.isna(val):
                    continue
                if "as of" in str(val).lower():
                    for date_str in self.extract_dates_from_text(val):
                        try:
                            val_date = pd.to_datetime(date_str).date()
                            if val_date == today or val_date == yesterday:
                                print(f"✅ Valid file found: {entry.name}")
                                return True
                        except:
                            continue

        print("❌ No valid file found with today's date")
        return False


    # =================================================
    # LOAD DB DATA
    # =================================================
    def load_db_data(self):

        conn  = psycopg2.connect(**self.db_config)
        query = """
        SELECT DISTINCT onsiteid, property_name
        FROM analytics.vw_bi_dim_info_property
        WHERE Original_Sold_Date IS NULL
          AND acquisition_date   IS NOT NULL
          AND sold_date          IS NULL
          AND investor           != 'Livcor'
        """
        df = pd.read_sql(query, conn)
        conn.close()

        df["onsiteid"]      = df["onsiteid"].astype(str)
        df["property_norm"] = df["property_name"].apply(self.normalize)
        self.df_db = df


    # =================================================
    # LOAD FILES
    # FIX: Removed incorrect .split("_")[0] on group_name
    # =================================================
    def load_files(self):

        records = []

        for filename in os.listdir(self.folder_path):

            name_without_ext = os.path.splitext(filename)[0]
            # Normalize spaces around underscores
            name_clean = re.sub(r"\s*_\s*", "_", name_without_ext.strip())
            parts = name_clean.split("_", 2)

            if len(parts) == 3:
                records.append({
                    "onsiteid":     parts[0].strip(),
                    "property_name": parts[1].strip(),
                    "group_name":   parts[2].strip(),   # FIX: removed wrong .split("_")[0]
                    "filename":     filename
                })

        df = pd.DataFrame(records)

        if not df.empty:
            df["onsiteid"]      = df["onsiteid"].astype(str)
            df["property_norm"] = df["property_name"].apply(self.normalize)
            df["group_norm"]    = df["group_name"].apply(self.normalize)

        self.df_files = df


    # =================================================
    # COMPARE LOGIC
    # FIX: Now checks missing groups per property individually
    #      instead of only flagging fully missing properties
    # =================================================
    def compare(self):

        missing_records = []

        # Build set of onsiteids covered by combined property map
        covered_onsiteids = set()
        for combined in self.COMBINED_PROPERTY_MAP.keys():
            combined_norm = self.normalize(combined)
            match = self.df_files[self.df_files["property_norm"] == combined_norm]
            for _, row in match.iterrows():
                covered_onsiteids.add(row["onsiteid"])

        # For each DB property check every required group individually
        for _, db_row in self.df_db.iterrows():

            onsiteid = db_row["onsiteid"]
            prop     = db_row["property_name"]

            # Skip properties covered by combined map
            if onsiteid in covered_onsiteids:
                continue

            for group in self.required_groups:
                group_norm = self.normalize(group)

                # Check if this group exists for this onsiteid
                match = self.df_files[
                    (self.df_files["onsiteid"] == onsiteid) &
                    (self.df_files["group_norm"] == group_norm)
                ]

                if match.empty:
                    missing_records.append({
                        "onsiteid":     onsiteid,
                        "property_name": prop,
                        "group_name":   group,
                        "filename":     ""
                    })
                    print(f"  ❌ Missing: onsiteid={onsiteid} | {prop} | {group}")

        self.df_missing_files = pd.DataFrame(missing_records)

        if self.df_missing_files.empty:
            print("✅ All properties have all required group files.")


    # =================================================
    # SAVE REPORTS
    # =================================================
    def save_reports(self):

        attachments = []

        if self.df_missing_files is not None and not self.df_missing_files.empty:
            file1 = f"missing_property_files_{os.path.basename(self.folder_path)}.csv"
            self.df_missing_files.to_csv(file1, index=False)
            attachments.append(file1)
            print(f"📄 Report saved: {file1} ({len(self.df_missing_files)} missing entries)")

        return attachments


    # =================================================
    # EMAIL
    # =================================================
    def send_email(self, attachments=None, date_flag=True):

        attachments = attachments or []

        # Nothing wrong → no email
        if date_flag and not attachments:
            print("✅ Data is correct. No email sent.")
            return

        msg = EmailMessage()
        msg["Subject"] = f"⚠️ BI Reconciliation – {os.path.basename(self.folder_path)}"
        msg["From"]    = self.sender_email
        msg["To"]      = ", ".join(self.recipients)

        issues = []
        if not date_flag:
            issues.append("❌ Latest file does not contain today's or yesterday's AS OF DATE")
        if attachments:
            issues.append("❌ Missing property files detected")

        body = f"""
BI File Validation Alert

Folder: {self.folder_path}

Issues detected:
{chr(10).join(issues)}

Please review the issue. If discrepancy files are present, they are attached.
"""
        msg.set_content(body)

        for file in attachments:
            try:
                with open(file, "rb") as f:
                    msg.add_attachment(
                        f.read(),
                        maintype="text",
                        subtype="csv",
                        filename=os.path.basename(file)
                    )
            except Exception as e:
                print(f"⚠️ Could not attach {file}: {e}")

        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)
            print("📧 Email sent")
        except Exception as e:
            print(f"❌ Email sending failed: {e}")


    # =================================================
    # RUN
    # FIX: validate_today_file() called only once
    # =================================================
    def run(self):

        # FIX: call once and store result
        date_flag = self.validate_today_file()

        if not date_flag:
            self.send_email(None, date_flag=False)
            print("❌ Date validation failed. Skipping reconciliation.")
            return

        self.load_db_data()
        self.load_files()
        self.compare()

        attachments = self.save_reports()
        self.send_email(attachments, date_flag=date_flag)


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":

    db_config = {
        "host":     "10.200.155.5",
        "database": "Olympus_Property",
        "user":     "pwalgude",
        "password": "Re@donly123",
        "port":     5432
    }

    smtp_server     = "smtp.office365.com"
    smtp_port       = 587
    sender_email    = "pratiksha.walgude@olympusproperty.com"
    sender_password = "Olympus@1510"

    recipients = [
        "pratiksha.walgude@olympusproperty.com"
        # "Theresa.Morgan@olympusproperty.com",
        # "kamal.bisht@olympusproperty.com"
    ]

    for folder_path in file_details.keys():

        print(f"\n🚀 Running reconciliation for: {folder_path}")

        reconciler = PropertyFileReconciliation(
            db_config=db_config,
            folder_path=folder_path,
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            sender_email=sender_email,
            sender_password=sender_password,
            recipients=recipients,
            file_details=file_details
        )

        reconciler.run()
