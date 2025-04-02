# ChatSheetsAI

Create a spreadsheet-like application (similar to Excel or Google Sheets) but entirely driven by a chat interface.

---

## Step 1

![step1](img/step1.png)

Manually load a CSV into SQLite and perform basic queries.

---

## Step 2

![step2](img/step2.png)

Infer schema from a CSV and create tables automatically.

---

## Step 3

If there are any differences between the CSV columns and the existing table columns, prompt the user to overwrite, rename, or skip.

![step3](img/step3.png)

---

## Step 4

![step4](img/step4.png)

Create a simple CLI that allows basic chat-like interaction: load CSV, list tables, run SQL queries, etc.

---

## Step 5

![step5](img/step5.png)

Integrate OpenAI to convert natural language queries into SQL commands.

---

### Quick Start
1. clone the repo

2. run ```pip install -r requirements.txt```

3. run the (for example) step1 script:<br>
```python step1.py --csv_file example_data.csv --db_name data.db```