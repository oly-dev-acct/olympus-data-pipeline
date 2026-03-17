Create or Alter Procedure [dbo].[SP_insert_update_fact_lease_expiration_renewal]
as
begin
SET NOCOUNT ON
	
--creating tmp table


	create table #TMP_TABLE_LEASE_EXP_RE(
							site_id_property_unit_number [varchar](50) not null
						  ,[FloorPlan]  [varchar](20)
						  ,[Name] [varchar](200)
						  ,[Actual rent] [varchar](10)
						  ,[Other Billings] [varchar](10)
						  ,[Last Increase] [varchar](10)
						  ,[Last Increase Amount] [varchar](10)
						  ,[Market Rent] [varchar](10)
						  ,[Move in Date] [varchar](10)
						  ,[Lease end Date] [varchar](10)
						  ,[Decision] [varchar](20)
						  ,[New Lease Start Date] [varchar](10)
						  ,[New Lease Term] [varchar](5)
						  ,[New Rent] [varchar](10)
						  ,[New Other Billings] [varchar](10)
						  ,[Leasing Consultant] [varchar](100)

	)


			insert into #TMP_TABLE_LEASE_EXP_RE(
							site_id_property_unit_number
						  ,[FloorPlan]
						  ,[Name]
						  ,[Actual rent]
						  ,[Other Billings]
						  ,[Last Increase]
						  ,[Last Increase Amount]
						  ,[Market Rent]
						  ,[Move in Date]
						  ,[Lease end Date]
						  ,[Decision]
						  ,[New Lease Start Date]
						  ,[New Lease Term]
						  ,[New Rent]
						  ,[New Other Billings]
						  ,[Leasing Consultant]
							)
							select 
						   site_id_property_unit_number
						  ,[FloorPlan]
						  ,[Name]
						  ,[Actual rent]
						  ,[Other Billings]
						  ,[Last Increase]
						  ,[Last Increase Amount]
						  ,[Market Rent]
						  ,[Move in Date]
						  ,[Lease end Date]
						  ,[Decision]
						  ,[New Lease Start Date]
						  ,[New Lease Term]
						  ,[New Rent]
						  ,[New Other Billings]
						  ,[Leasing Consultant]
					from [Olympus_Property_Staging].[dbo].[lease_expiration_stage] LES
					LEFT JOIN [Olympus_Property_Analytics].[dbo].[dim_unit] DU on LES.[On Site id]=DU.onsiteid and LES.[Bldg/Unit]=DU.[unit_number]
		--cleaning tmp table 
					UPDATE #TMP_TABLE_LEASE_EXP_RE
					SET site_id_property_unit_number=NULLIF(site_id_property_unit_number, 'None')
					   ,[FloorPlan]=NULLIF([FloorPlan], 'None')
					   ,[Name]=NULLIF([Name], 'None')
					   ,[Actual rent]=NULLIF([Actual rent], 'None')
					   ,[Other Billings]=NULLIF([Other Billings], 'None')
					   ,[Last Increase]=NULLIF([Last Increase], 'None')
					   ,[Last Increase Amount]=NULLIF([Last Increase Amount], 'None')
					   ,[Market Rent]=NULLIF([Market Rent], 'None')
					   ,[Move in Date]=NULLIF([Move in Date], 'None')
					   ,[Lease end Date]=NULLIF([Lease end Date], 'None')
					   ,[Decision]=NULLIF([Decision], 'None')
					   ,[New Lease Start Date]=NULLIF([New Lease Start Date], 'None')
					   ,[New Lease Term]=NULLIF([New Lease Term], 'None')
					   ,[New Rent]=NULLIF([New Rent], 'None')
					   ,[New Other Billings]=NULLIF([New Other Billings], 'None')
					   ,[Leasing Consultant]=NULLIF([Leasing Consultant], 'None')


					UPDATE #TMP_TABLE_LEASE_EXP_RE
					SET site_id_property_unit_number=NULLIF(site_id_property_unit_number, '')
					   ,[FloorPlan]=NULLIF([FloorPlan], '')
					   ,[Name]=NULLIF([Name], '')
					   ,[Actual rent]=NULLIF([Actual rent], '')
					   ,[Other Billings]=NULLIF([Other Billings], '')
					   ,[Last Increase]=NULLIF([Last Increase], '')
					   ,[Last Increase Amount]=NULLIF([Last Increase Amount], '')
					   ,[Market Rent]=NULLIF([Market Rent], '')
					   ,[Move in Date]=NULLIF([Move in Date], '')
					   ,[Lease end Date]=NULLIF([Lease end Date], '')
					   ,[Decision]=NULLIF([Decision], '')
					   ,[New Lease Start Date]=NULLIF([New Lease Start Date], '')
					   ,[New Lease Term]=NULLIF([New Lease Term], '')
					   ,[New Rent]=NULLIF([New Rent], '')
					   ,[New Other Billings]=NULLIF([New Other Billings], '')
					   ,[Leasing Consultant]=NULLIF([Leasing Consultant], '')










			insert into [Olympus_Property_Analytics].[dbo].[fact_lease_expiration_renewal](
										   site_id_property_unit_number
										  ,[floor_plan]
										  ,[name]
										  ,[actual_rent]
										  ,[other_billings]
										  ,[last_increase]
										  ,[last_increase_amount]
										  ,[market_rent]
										  ,[move_in_date]
										  ,[lease_end_date]
										  ,[decision]
										  ,[new_lease_start_date]
										  ,[new_lease_term]
										  ,[new_rent]
										  ,[new_other_billings]
										  ,[leasing_consultant])


						   select 	
						   cast (T.site_id_property_unit_number AS varchar(30)) as [Bldg/Unit]
						  ,cast (T.[FloorPlan] AS varchar(20)) as [FloorPlan]
						  ,cast (T.[Name] AS [varchar](200)) as [Name]
						  ,cast (T.[Actual rent] AS money) as [Actual rent]
						  ,cast (T.[Other Billings] AS money) as [Other Billings]
						  ,cast (T.[Last Increase] AS date) as [Last Increase]
						  ,cast (T.[Last Increase Amount] AS money) as [Last Increase Amount]
						  ,cast (T.[Market Rent] AS money) as [Market Rent]
						  ,cast (T.[Move in Date] AS date) as [Move in Date]
						  ,cast (T.[Lease end Date] AS date) as [Lease end Date]
						  ,cast (T.[Decision]  as varchar(20)) as [Decision]
						  ,cast (T.[New Lease Start Date] AS date) as [New Lease Start Date]
						  ,cast (T.[New Lease Term] as float) as [New Lease Term]
						  ,cast (T.[New Rent] as money ) as [New Rent]
						  ,cast (T.[New Other Billings] as money ) as [New Other Billings]
						  , cast (T.[Leasing Consultant] as [varchar](100)) as [Leasing Consultant]
						  from #TMP_TABLE_LEASE_EXP_RE  T
						  except 
						 select
						   site_id_property_unit_number
						  ,[floor_plan]
						  ,[name]
						  ,[actual_rent]
						  ,[other_billings]
						  ,[last_increase]
						  ,[last_increase_amount]
						  ,[market_rent]
						  ,[move_in_date]
						  ,[lease_end_date]
						  ,[decision]
						  ,[new_lease_start_date]
						  ,[new_lease_term]
						  ,[new_rent]
						  ,[new_other_billings]
						  ,[leasing_consultant]
						From [Olympus_Property_Analytics].[dbo].[fact_lease_expiration_renewal]

		UPDATE [Olympus_Property_Analytics].[dbo].[fact_lease_expiration_renewal]
		SET [new_lease_end_date]=DATEADD(MONTH, [new_lease_term], [new_lease_start_date])
		WHERE [new_lease_start_date] IS NOT NULL AND [new_lease_term] IS NOT NULL 



		DROP TABLE #TMP_TABLE_LEASE_EXP_RE



end