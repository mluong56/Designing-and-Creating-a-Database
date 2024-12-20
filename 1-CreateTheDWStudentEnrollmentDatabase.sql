--**************************************************************************--
-- Title: Create the DWStudentEnrollments database
-- Desc: This file will drop and create the DWStudentEnrollments database. 
-- 2024-12-15,MLuong,Created starter code
--**************************************************************************--
Set NoCount On;
Go

USE [master]
Go

If Exists (Select Name from SysDatabases Where Name = 'DWStudentEnrollments')
  Begin
   ALTER DATABASE DWStudentEnrollments SET SINGLE_USER WITH ROLLBACK IMMEDIATE
   DROP DATABASE DWStudentEnrollments
  End
Go

Create Database DWStudentEnrollments;
Go

USE DWStudentEnrollments;
Go

--********************************************************************--
-- Create the Tables
--********************************************************************--

Create Table dbo.DimClasses (
 ClassKey int Not Null Constraint pkDimClasses Primary Key Identity(1,1) -- AutoNumber
,ClassID int Not Null 
,ClassName nvarchar (200) Not Null 
,ClassStartDate date Not Null
,ClassEndDate date Not Null
,CurrentClassPrice money Not Null
,MaxClassEnrollment int Not Null
,ClassroomId int Not Null
,ClassroomName nvarchar(100) Not Null
,ClassroomMaxSize int Not Null
,DepartmentId int Not Null
,DepartmentName nvarchar(100) Not Null
);
Go

Create Table dbo.DimStudents ( 
 StudentKey int Not Null Constraint pkDimStudents Primary Key Identity(1,1) -- AutoNumber
,StudentId int Not Null
,StudentName nvarchar(100) Not Null
);
Go

Create Table dbo.FactEnrollments ( 
 EnrollmentId int Not Null
,EnrollmentDateKey int Not Null
,StudentKey int Not Null -- To Surregate Key
,ClassKey int Not Null -- To Surregate Key
,EnrollmentPrice money Not Null -- Measure
Constraint pkFactEnrollments Primary Key 
 (EnrollmentId, EnrollmentDateKey, StudentKey, ClassKey)
);
Go

Create Table dbo.DimDates ( 
 DateKey int Not Null Constraint pkDimDates Primary Key -- SmartKey YYYYMMDD 
,FullDate date Not Null
,FullDateName nvarchar(100) Not Null
,MonthKey int Not Null
,MonthName nvarchar(100) Not Null
,QuarterKey int Not Null
,QuarterName nvarchar(100) Not Null
,YearKey int Not Null
,YearName nvarchar(100) Not Null
);
Go

--********************************************************************--
-- Create the FOREIGN KEY CONSTRAINTS
--********************************************************************--
Alter Table dbo.FactEnrollments
 Add Constraint fkDimClasses Foreign Key (ClassKey) 
  References dbo.DimClasses(ClassKey);
Go

Alter Table dbo.FactEnrollments
 Add Constraint fkDimStudents Foreign Key (StudentKey) 
  References dbo.DimStudents(StudentKey);
Go

Alter Table dbo.FactEnrollments
 Add Constraint fkDimDates Foreign Key (EnrollmentDateKey) 
  References dbo.DimDates(DateKey);
Go

--********************************************************************--
-- Create the Abstraction Layers
--********************************************************************--

-- Base Views
Create View dbo.vDimClasses
As
 Select 
  ClassKey 
 ,ClassID 
 ,ClassName
 ,ClassStartDate
 ,ClassEndDate
 ,CurrentClassPrice
 ,MaxClassEnrollment
 ,ClassroomId
 ,ClassroomName
 ,ClassroomMaxSize
 ,DepartmentId
 ,DepartmentName
 From dbo.DimClasses;
Go

Create View dbo.vDimStudents
As
 Select
  StudentKey
 ,StudentId
 ,StudentName
 From dbo.DimStudents;
Go

Create View dbo.vFactEnrollments
As
 Select
  EnrollmentId
 ,EnrollmentDateKey
 ,StudentKey
 ,ClassKey
 ,EnrollmentPrice
 From FactEnrollments;
Go

Create View dbo.vDimDates
As
 Select
  DateKey
 ,FullDate
 ,FullDateName
 ,MonthKey
 ,MonthName
 ,QuarterKey
 ,QuarterName
 ,YearKey
 ,YearName
 From dbo.DimDates;
Go

-- Metadata View
Go
Create or Alter View vMetaDataStudentEnrollments
As
 Select Top 100 Percent
  [Source Table] = DB_Name() + '.' + SCHEMA_NAME(tab.[schema_id]) + '.' + object_name(tab.[object_id])
 ,[Source Column] =  col.[Name]
 ,[Source Type] = Case 
     When t.[Name] in ('char', 'nchar', 'varchar', 'nvarchar' ) 
       Then t.[Name] + ' (' +  format(col.max_length, '####') + ')'                
     When t.[Name]  in ('decimal', 'money') 
       Then t.[Name] + ' (' +  format(col.[precision], '#') + ',' + format(col.scale, '#') + ')'
      Else t.[Name] 
                 End 
 ,[Source Nullability] = iif(col.is_nullable = 1, 'Null', 'Not Null') 
 From Sys.Types as t 
 Join Sys.Columns as col 
  On t.system_type_id = col.system_type_id 
 Join Sys.Tables tab
   On tab.[object_id] = col.[object_id]
 And t.name <> 'sysname'
 Order By [Source Table], col.column_id; 
Go

Select * From vMetaDataStudentEnrollments;
Go
