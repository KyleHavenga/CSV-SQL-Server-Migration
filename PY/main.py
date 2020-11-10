import pandas as pd
import pyodbc

server = 'itdevs.database.windows.net'
database = 'emp_info'
username = 'KyleHavenga'
password = 'HavengA2014'
driver= '{ODBC Driver 17 for SQL Server}'

connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+password)

cursor = connection.cursor()

data = pd.read_csv (r"C:\Users\Kyle\Documents\Programming\migration_CSV~SQL\Data\dataset.csv")
df = pd.DataFrame(data, columns= ["Age","Attrition","BusinessTravel"
,"DailyRate","Department","DistanceFromHome"
,"Education","EducationField","EmployeeCount"
,"EmployeeNumber","EnvironmentSatisfaction","Gender"
,"HourlyRate","JobInvolvement","JobLevel","JobRole","JobSatisfaction"
,"MaritalStatus","MonthlyIncome","MonthlyRate","NumCompaniesWorked"
,"Over18","OverTime","PercentSalaryHike","PerformanceRating","RelationshipSatisfaction"
,"StandardHours","StockOptionLevel","TotalWorkingYears","TrainingTimesLastYear","WorkLifeBalance"
,"YearsAtCompany","YearsInCurrentRole","YearsSinceLastPromotion","YearsWithCurrManager"
])

print(df)

for row in df.itertuples():
    cursor.execute('''
    INSERT INTO PrimaryTable (emp_number, marital_status, age, gender, over_18, num_companies_worked, num_working_years, 
    distance_from_home, education, education_field) VALUES (?,?,?,?,?,?,?,?,?,?) ''', 
    row.EmployeeNumber, 
    row.MaritalStatus, 
    row.Age, 
    row.Gender, 
    row.Over18, 
    row.NumCompaniesWorked, 
    row.TotalWorkingYears, 
    row.DistanceFromHome, 
    row.Education, 
    row.EducationField
    )
    connection.commit()

    cursor.execute('''
    INSERT INTO Rates (emp_number, hourly_rate, monthly_rate, daily_rate) VALUES (?,?,?,?) ''',
    row.EmployeeNumber,
    row.HourlyRate, 
    row.MonthlyRate, 
    row.DailyRate
    )
    connection.commit()

    cursor.execute('''
    INSERT INTO jobdetails (emp_number, attrition, business_travel, department, employee_count, job_involvement, job_level, 
    job_role, overtime, standard_hours, stock_option_level, years_last_promotion, years_current_role, years_current_manager, 
    years_at_company)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ''',
    row.EmployeeNumber,
    row.Attrition, 
    row.BusinessTravel, 
    row.Department, 
    row.EmployeeCount,
    row.JobInvolvement, 
    row.JobLevel, 
    row.JobRole, 
    row.OverTime, 
    row.StandardHours, 
    row.StockOptionLevel, 
    row.YearsSinceLastPromotion, 
    row.YearsInCurrentRole, 
    row.YearsWithCurrManager, 
    row.YearsAtCompany
    )
    connection.commit()

    cursor.execute('''
    INSERT INTO EmployeePerf (emp_number, environment_sat, job_sat, relationship_sat, work_life_balance)
    VALUES (?,?,?,?,?)''',
    row.EmployeeNumber, 
    row.EnvironmentSatisfaction, 
    row.JobSatisfaction, 
    row.RelationshipSatisfaction, 
    row.WorkLifeBalance
    )
    connection.commit()

    cursor.execute('''
    INSERT INTO ManagerRating (emp_number, performance_rating, training_times_last_year) VALUES (?,?,?) ''', 
    row.EmployeeNumber, 
    row.PerformanceRating, 
    row.TrainingTimesLastYear
    )
    connection.commit()