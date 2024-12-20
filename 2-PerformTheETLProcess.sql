--**************************************************************************--
-- Title: DWStudentEnrollments ETL Process
-- Desc: This file performs ETL processing for the DWStudentEnrollments database. 
-- 2024-12-15,MLuong,Created starter code
--**************************************************************************--

USE DWStudentEnrollments;
Go
Set NoCount On;
Go

--********************************************************************--
-- 0) Create ETL metadata objects
--********************************************************************--
If NOT Exists(Select * From Sys.tables where Name = 'EtlLog')
  Create -- Drop
  Table EtlLog
  (EtlLogID int identity Primary Key
  ,ETLDateAndTime datetime Default GetDate()
  ,ETLAction varchar(100)
  ,EtlLogMessage varchar(2000)
  );
go

Create or Alter View vEtlLog
As
  Select
   EtlLogID
  ,ETLDate = Format(ETLDateAndTime, 'D', 'en-us')
  ,ETLTime = Format(Cast(ETLDateAndTime as datetime2), 'HH:mm:ss', 'en-us')
  ,ETLAction
  ,EtlLogMessage
  From EtlLog;
go

Create or Alter Proc pInsEtlLog
 (@ETLAction varchar(100), @EtlLogMessage varchar(2000))
--*************************************************************************--
-- Desc:This Sproc creates an admin table for logging ETL metadata. 
-- 2024-12-01,MLuong,Created Sproc
--*************************************************************************--
As
Begin
  Declare @RC int = 0;
  Begin Try
    Begin Tran;
      Insert Into EtlLog
       (ETLAction,EtlLogMessage)
      Values
       (@ETLAction,@EtlLogMessage)
    Commit Tran;
    Set @RC = 1;
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Set @RC = -1;
  End Catch
  Return @RC;
End
Go
-- Truncate Table ETLLog;
-- Exec pInsETLLog @ETLAction = 'Begin ETL',@ETLLogMessage = 'Start of ETL process' 
-- Select * From vEtlLog

--********************************************************************--
-- Pre-load tasks
--********************************************************************--
Go
Create or Alter Proc pEtlDropFks
--*************************************************************************--
-- Desc:This sproc drop the foreign key constaints
-- 2024-12-01,MLuong,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000) 
  Begin Try
    Alter Table dbo.FactEnrollments Drop Constraint fkDimClasses;
    Alter Table dbo.FactEnrollments Drop Constraint fkDimStudents ;
    Alter Table dbo.FactEnrollments Drop Constraint fkDimDates;
    Set @RC = 1
  End Try
  Begin Catch
    Declare @ErrorMessage nvarchar(1000) = Error_Message();
	  Exec pInsEtlLog
	        @ETLAction = 'pEtlDropFks'
	       ,@EtlLogMessage = @ErrorMessage;
    Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go
-- Exec pEtlDropFks; Select * From vEtlLog;
Go
Create or Alter Proc pEtlTruncateTables
--*************************************************************************--
-- Desc:This sproc truncates the data from all the tables
-- 2024-12-01,MLuong,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000) 
  Begin Try
    Truncate Table dbo.DimClasses;
    Truncate Table dbo.DimStudents ;
    Truncate Table dbo.DimDates;
    Truncate Table dbo.FactEnrollments;
    Set @RC = 1
  End Try
  Begin Catch
    Declare @ErrorMessage nvarchar(1000) = Error_Message();
	  Exec pInsEtlLog
	        @ETLAction = 'pEtlTruncateTables'
	       ,@EtlLogMessage = @ErrorMessage;
    Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go
-- Exec pEtlTruncateTables; Select * From vEtlLog;

--********************************************************************--
-- Load dimension tables
--********************************************************************--
Go
Create or Alter Proc pEtlDimDates
--*************************************************************************--
-- Desc:This sproc generates date data for the DimDates tables
-- 2024-12-10,MLuong,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 1;
  Declare @Message varchar(1000) 
  Set NoCount On; -- This will remove the 1 row affected msg in the While loop;
  Begin Try
 	  -- Create variables to hold the start and end date
	  Declare @StartDate datetime = '01/01/2015';
	  Declare @EndDate datetime = '12/31/2025'; 
	  Declare @DateInProcess datetime;
    Declare @TotalRows int = 0;

	  -- Use a while loop to add dates to the table
	  Set @DateInProcess = @StartDate;

	  While @DateInProcess <= @EndDate
	    Begin
	      -- Add a row into the date dimensiOn table for this date
	     Begin Tran;
	       Insert Into DimDates 
	       ( [DateKey], [FullDate], [FullDateName], [MonthKey], [MonthName], [QuarterKey], [QuarterName], [YearKey], [YearName] )
	       Values ( 
	   	     Cast(Convert(nvarchar(50), @DateInProcess , 112) as int) -- [DateKey]
	        ,@DateInProcess -- [FullDate]
	        ,DateName( weekday, @DateInProcess ) + ', ' + Convert(nvarchar(50), @DateInProcess , 110) -- [USADateName]  
	        ,Left(Cast(Convert(nvarchar(50), @DateInProcess , 112) as int), 6) -- [MonthKey]   
	        ,DateName( MONTH, @DateInProcess ) + ', ' + Cast( Year(@DateInProcess ) as nVarchar(50) ) -- [MonthName]
	        , Cast(Cast(YEAR(@DateInProcess) as nvarchar(50))  + '0' + DateName( QUARTER,  @DateInProcess) as int) -- [QuarterKey]
	        ,'Q' + DateName( QUARTER, @DateInProcess ) + ', ' + Cast( Year(@DateInProcess) as nVarchar(50) ) -- [QuarterName] 
	        ,Year( @DateInProcess ) -- [YearKey]
	        ,Cast( Year(@DateInProcess ) as nVarchar(50) ) -- [YearName] 
	        ); 
	       -- Add a day and loop again
	       Set @DateInProcess = DateAdd(d, 1, @DateInProcess);
	     Commit Tran;
      Set @TotalRows += 1;
	  End -- While
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Declare @ErrorMessage nvarchar(1000) = Error_Message();
	  Exec pInsEtlLog
	        @ETLAction = 'pEtlDimDates'
	       ,@EtlLogMessage = @ErrorMessage;
    Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go
-- Exec pEtlDimDates; Select * From DimDates;Select * From vEtlLog;

Go
Create or Alter View vEtlDimClasses
As 
SELECT c.* FROM OPENROWSET('SQLNCLI11'
,'Server=bidd-24-25.database.windows.net;uid=biddadmin;pwd=biddP@$$word;database=StudentEnrollments;' 
, 'Select 
 [ClassID] = c.[Id]
,[ClassName] = c.[Name]
,[ClassStartDate] = c.[StartDate]
,[ClassEndDate] = c.[EndDate]
,[CurrentClassPrice] = c.[Price]
,[MaxClassEnrollment] = c.[MaxSize]
,[ClassroomId] = cl.[Id]
,[ClassroomName] = cl.[Name]
,[ClassroomMaxSize] = cl.[MaxSize]
,[DepartmentId] = d.[Id]
,[DepartmentName] = d.[Name]
From [dbo].[Classes] as c
Join [dbo].[Classrooms] as cl On c.[ClassroomId] = cl.[Id]
Join [dbo].[Departments] as d On c.[DepartmentId] = d.[Id]'
) AS c;
Go
-- Select * From vEtlDimClasses

Create or Alter Proc pEtlDimClasses
--*************************************************************************--
-- Desc:This sproc fills DimClasses
-- Change Log: When,Who,What
-- 2024-12-12,MLuong,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000) 
  Begin Try
    Begin Tran;
      Insert Into [dbo].[DimClasses]
      ( [ClassID]
      , [ClassName]
      , [ClassStartDate]
      , [ClassEndDate]
      , [CurrentClassPrice]
      , [MaxClassEnrollment]
      , [ClassroomId]
      , [ClassroomName]
      , [ClassroomMaxSize]
      , [DepartmentId]
      , [DepartmentName] 
      ) Select 
        [ClassID]
      , [ClassName]
      , [ClassStartDate]
      , [ClassEndDate]
      , [CurrentClassPrice]
      , [MaxClassEnrollment]
      , [ClassroomId]
      , [ClassroomName]
      , [ClassroomMaxSize]
      , [DepartmentId]
      , [DepartmentName] 
      From vETLDimClasses;
    Commit Tran;
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Declare @ErrorMessage nvarchar(1000) = Error_Message();
	  Exec pInsEtlLog
	        @ETLAction = 'pEtlDimClasses'
	       ,@EtlLogMessage = @ErrorMessage;
    Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go
-- Exec pEtlDimClasses; Select * From vEtlLog;

Create Or Alter View vETLDimStudents
As
  SELECT s.* FROM OPENROWSET('SQLNCLI11'
  ,'Server=bidd-24-25.database.windows.net;uid=biddadmin;pwd=biddP@$$word;database=StudentEnrollments;' 
  , 'Select 
      [StudentId] = [Id]
     ,[StudentName] = Cast(([FirstName] + '' '' + [LastName]) as nVarchar(100))
     From [dbo].[Students]'
  ) AS s;
Go

Create or Alter Proc pEtlDimStudents
--*************************************************************************--
-- Desc:This sproc fills DimStudents
-- Change Log: When,Who,What
-- 2024-12-12,MLuong,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000) 
  Begin Try
    Begin Tran;
      Insert Into DimStudents
      ([StudentId], [StudentName])
      Select 
      [StudentId], [StudentName]
      From vETLDimStudents
    Commit Tran;
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Declare @ErrorMessage nvarchar(1000) = Error_Message();
	  Exec pInsEtlLog
	        @ETLAction = 'pEtlDimStudents'
	       ,@EtlLogMessage = @ErrorMessage;
    Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go
-- Exec pEtlDimStudents; Select * From vEtlLog;


--********************************************************************--
-- Load Fact Tables
--********************************************************************--
Go
Create Or Alter View vETLFactEnrollments
As 
SELECT 
  [EnrollmentId] = fe.[EnrollmentId]
, [EnrollmentDateKey] = dd.[DateKey]
, [StudentKey] = ds.[StudentKey]
, [ClassKey] = dc.[ClassKey]
, [EnrollmentPrice] = fe.[EnrollmentPrice] --, '-------------' as AllColumns, fe.*, ds.*, dc.*
  FROM OPENROWSET('SQLNCLI11'
        ,'Server=bidd-24-25.database.windows.net;uid=biddadmin;pwd=biddP@$$word;database=StudentEnrollments;' 
        , 'Select
           [EnrollmentId] = [Id]
          ,[Date]
          ,[StudentId]
          ,[ClassId]
          ,[EnrollmentPrice] =[Price]
          From [dbo].[Enrollments]'
        ) AS fe
  JOIN [dbo].[DimStudents] as ds
    On fe.[StudentId] = ds.StudentId
  JOIN [dbo].[DimClasses] as dc
    On fe.[ClassId] = dc.ClassId
  JOIN [dbo].[DimDates] as dd
    On Cast(fe.[Date] as date) = dd.FullDate
    ;
Go
-- Select * From vETLFactEnrollments


Create or Alter Proc pEtlFactEnrollments
--*************************************************************************--
-- Desc:This sproc fills FactEnrollments
-- Change Log: When,Who,What
-- 2024-12-12,MLuong,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 0;
  Declare @Message varchar(1000) 
  Begin Try
    Begin Tran;
      Insert Into [dbo].[FactEnrollments]
      ([EnrollmentId], [EnrollmentDateKey], [StudentKey], [ClassKey], [EnrollmentPrice])
      Select
      [EnrollmentId], [EnrollmentDateKey], [StudentKey], [ClassKey], [EnrollmentPrice]
      From vETLFactEnrollments
    Commit Tran;
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Declare @ErrorMessage nvarchar(1000) = Error_Message();
	  Exec pInsEtlLog
	        @ETLAction = 'pEtlFactEnrollments'
	       ,@EtlLogMessage = @ErrorMessage;
    Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go
-- Exec pEtlFactEnrollments; Select * From vEtlLog;






--********************************************************************--
-- Post-load Tasks
--********************************************************************--
Create or Alter Proc pEtlReplaceFKs
--*************************************************************************--
-- Desc:This sproc replaces the foreign key constaints
-- Change Log: When,Who,What
-- 2024-12-12,MLuong,Created Sproc
--*************************************************************************--
As 
Begin
  Declare @RC int = 1;
  Declare @Message varchar(1000) 
  Begin Try
    Alter Table dbo.FactEnrollments
      Add Constraint fkDimClasses Foreign Key (ClassKey) 
        References dbo.DimClasses(ClassKey);

   Alter Table dbo.FactEnrollments
     Add Constraint fkDimStudents Foreign Key (StudentKey) 
       References dbo.DimStudents(StudentKey);

   Alter Table dbo.FactEnrollments
     Add Constraint fkDimDates Foreign Key (EnrollmentDateKey) 
       References dbo.DimDates(DateKey);
  End Try
  Begin Catch
    Declare @ErrorMessage nvarchar(1000) = Error_Message();
	  Exec pInsEtlLog
	        @ETLAction = 'pEtlDropFks'
	       ,@EtlLogMessage = @ErrorMessage;
    Set @RC = -1;
  End Catch
  Set NoCount Off;
  Return @RC;
End
Go

--********************************************************************--
-- Review the results of this script
--********************************************************************--
Go
Exec pInsETLLog @ETLAction = 'Begin ETL', @ETLLogMessage = 'Start of ETL process' 
Exec pEtlDropFks; 
Exec pEtlTruncateTables;
Exec pEtlDimDates; Select top 10 * From vDimDates;
Exec pEtlDimClasses; Select * From vDimClasses;
Exec pEtlDimStudents; Select * From vDimStudents;
Exec pEtlFactEnrollments; Select * From vFactEnrollments;
Exec pEtlReplaceFKs;
Exec pInsETLLog @ETLAction = 'End ETL', @ETLLogMessage = 'End of ETL process' 
Select * From vEtlLog



