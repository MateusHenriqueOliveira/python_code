--Analytics
--commonly regions
select count(region), region from jobsity.dbo.trips
group by region

--regions appaer cheap_mobile
select region from jobsity.dbo.trips
where datasource = 'cheap_mobile'
group by region