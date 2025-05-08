--*************************************************************************--
-- Title: ETLViews For DWStudentEnrollments Tabular Model
-- Desc:This file will drop and create views for DWStudentEnrollments Tabular Model. 
-- Change Log: When,Who,What
-- 2025-01-01,MLUong,Created File
--*************************************************************************--

--Step 1: The Tabular ETL Script
--Create a SQL script with Tabular ETL views to shape the data for your Tabular Model Name the script "3-ETLViewsForTabularModel.sql". See Module05 for examples of what should be in the views.
--Notes: 
--•	Create ETL views to load data into your Tabular Model 
--•	Use Convert(date, Cast([EnrollmentDateKey] as char(8)), 110) to convert the DateKey and EnrollmentDateKey columns to date. 

USE [DWStudentEnrollments]
GO

-- Base Views
Create or Alter View [dbo].[vTabularETLDimClasses]
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
GO
-- Select * From [dbo].[vDimClasses]

CREATE OR ALTER View [dbo].[vTabularETLDimStudents]
As
 Select
  StudentKey
 ,StudentId
 ,StudentName
 From dbo.DimStudents;
GO

CREATE OR ALTER View [dbo].[vTabularETLDimDates]
As
 Select
  -- Convert(date, Cast([DateKey] as char(8)), 110) as DateKey
  FullDate
 ,FullDateName
 ,MonthKey
 ,MonthName
 ,QuarterKey
 ,QuarterName
 ,YearKey
 ,YearName
 From dbo.DimDates;
Go

CREATE OR ALTER View [dbo].[vTabularETLFactEnrollments]
As
 Select
  EnrollmentId
 ,Convert(date, Cast([EnrollmentDateKey] as char(8)), 110) as EnrollmentDateKey
 ,StudentKey
 ,ClassKey
 ,EnrollmentPrice
 From FactEnrollments;
GO




