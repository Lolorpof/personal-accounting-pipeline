# Midterm Report

**Student Name:** Sirawit Sirabanchongkran
**Student ID:** 650610810  
**Date:** 20/03/69

---

## Part 1: Data Exploration Answers

> How many total transactions are there?

_7,258,600 total transactions_

> How many unique family members, merchants, and categories?

_4 unique family members, 48 unique merchants, and 18 unique categories in transactions._

> How many rows have null or empty `amount` values?

_1,450,421 rows with null or empty 'amount'_

> What is the date range of the transactions?

_The date range in transactions is 2010-01-01 to 2025-12-31._

---

## Part 4: Join Analysis

> What join type did you use for enriching transactions, and why?

_Left join for both categories and merchants because We need to make sure we keep every transactions._

> How many transactions have no matching merchant in the merchants table?

_0 transactions, which means every transactions has a matching merchant._

> What would happen if you used an inner join instead?

_The transactions that don't have a matching merchant or category will be removed. But there's all transactions have a matched with merchant and category, so the results will be the same._

---

## Part 5: Analytics Insights

> Look at the average transaction amount per year table. Do you notice a trend? Calculate the approximate year-over-year percentage change. What might explain this?

_The spending by year increased overtime._

| Year | Avg Amount | YoY Change (%) |
| ---- | ---------- | -------------- |
| 2016 | 55.9825    | -              |
| 2017 | 57.1175    | 2.0274         |
| 2018 | 58.3302    | 2.1232         |
| 2019 | 59.4676    | 1.9498         |
| 2020 | 60.5863    | 1.8812         |
| 2021 | 61.8715    | 2.1213         |
| 2022 | 63.0809    | 1.9547         |
| 2023 | 64.4120    | 2.1102         |
| 2024 | 65.5414    | 1.7534         |
| 2025 | 66.9405    | 2.1347         |

_The spending by year shows sign of steadily increasing by year. If you calculate the yoy change, the percentage is always positive. which shows it increasing._

> Which category has the highest total spending? Which has grown the fastest over 10 years?

_'Groceries' is the category with the highest total spending, while 'Electronics' is the category with the fastest growth over 10 years._

> Compare spending between family members. Who spends the most? On what?

_MEM01 spends the most on 'Groceries'._

> What percentage of transactions fall in each spending tier? Has this distribution changed over the years?

_Overall, 14% falls in 'micro', 50% falls in 'small', 31% falls in 'medium', and 5% falls in 'large'. The distributions doesn't change much overall, with 'medium' and 'large' spending tier increasing, while 'micro' and 'small' spending tier decreasing._

---

## Section A: Data Architecture Questions

_The family has some questions about how the system works._

### Q1. Merchant Name Change

> "We just found out one of the merchants changed their name last year. Where in the pipeline do we update this, and what layers need to be reprocessed?"

_Merchants changing names means data in provided CSV file changes, which means we need to update/reprocessed everything where there's use of merchant data, from 'raw' layer all the way through 'analytics' layer._

---

### Q2. New Family Member

> "Our daughter started college and has her own credit card now. How do we add a new family member to the system without breaking existing data?"

_You just need to assigned your daughter to a new 'member_id'._

---

### Q3. Average Monthly Grocery Spending

> "We want to know our average monthly grocery spending. Walk us through exactly which transformations and joins produce this number."

_We use the staged transactions, which already joins every tables for us. We group the staged transactions by 'date(year and month)' and 'category_name', then we use aggregate function(sum) to sum the 'amount' to get total spending for each category for each month._

---

### Q4. Duplicate Transactions

> "Last month's bank export had 500 duplicate transactions. How does your pipeline handle this? If it doesn't yet, what would you add?"

_We can handle this issue by using dropDuplicate() function on 'transaction_id' column._

---

### Q5. Data Backup & Recovery

> "We're worried about losing our data. What's your backup strategy? What's the most data we could lose if something crashes?"

_Hint: Think about RPO (Recovery Point Objective) and RTO (Recovery Time Objective)._

_RPO = 1 month. The most data you could lose equals one month of transactions — since the pipeline runs monthly when the bank exports new data. If a crash happens just before backup, you lose that month's export. RTO depends on how fast the bank can export the data, because re-processing the raw data doesn't take a long time but if the bank can't export it fast enough, it will take a long time there._

---

## Section B: Engineering Questions

_The family's developer friend has some technical questions._

### Q6. CI/CD Pipeline

> "If we set up CI/CD for this project, what would the pipeline look like? What gets tested automatically, and what triggers the tests?"

_The pipeline starts by installing dependencies as the first step, then we should run the tests we implemented to see if it all passes, after that we import the new CSV files, then, we write the raw layer parquet from the imported CSV file, after that, we write staged layer parquet, lastly we write all the analytics we needs according to the business interest._

---

### Q7. Monthly Automation with Orchestration

> "We want this pipeline to run automatically every month when the bank exports new transactions. How would you set this up? Draw the DAG."

_Your answer (3–5 sentences):_

_Draw your DAG below (text-based diagram):_

```
Example format:
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Task 1  │ ──▶ │  Task 2  │ ──▶ │  Task 3  │
└──────────┘     └──────────┘     └──────────┘

Your DAG:

Install Dependencies ──▶ Import CSV files ──▶ Write raw parquet ──▶ Write staged parquet
                                                                              │
   ┌──────────────────────────────────────────────────────────────────────────┘
   └─▶ Write enriched & Analytics parquets
```

---

## Section C: Analytics Insights

_The family wants your professional opinion._

### Q8. Price Trend Analysis

> "We looked at your yearly average transaction table and prices seem to go up. Can you calculate the exact rate? Is it consistent across all categories?"

_Calculate the avg yoy change = (2.0274 + 2.1232 + 1.9498 + 1.8812 + 2.1213 + 1.9547 + 2.1102 + 1.7534 + 2.1347) / 9 = 2.01% per year_

_The total average YoY change is around 2.01%. For average YoY change per category, the lowest YoY is 0.24% and the highest is 0.35%, which is very consistent._

---

### Q9. Spending Recommendations

> "Based on your summary tables, give us 3 actionable recommendations for how we can reduce spending next year."

1. _See who's the family member with the highest spending, make them reduce their spending._
2. _Set a cap for category that has the highest spending._
3. _Try to reduce 'medium' and 'large' spending because those have been increasing over the years._

---

### Q10. Needs vs Wants

> "Which spending categories are 'needs' vs 'wants'? What percentage of our total spending goes to each?"

_There's already 'budget_type' column in our data, so we will use this to get the total spending for each budget type. Belows is the summary._

| Budget Type | Total Spending   | Percentage |
| ----------- | ---------------- | ---------- |
| Needs       | 2,160,425,364.36 | 49.77      |
| Wants       | 1,902,196,734.93 | 6.42       |
| Savings     | 278,489,059.78   | 43.82      |
