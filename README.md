# sbis_test
test task for sbis


# Run application
1. Run DB - docker exmaple: (`docker run --name your_name -p 5432:5432 -e POSTGRES_USER=your_user -e POSTGRES_PASSWORD=your_password -e POSTGRES_DB=your_db -d postgres:10.16-alpine`)
2. Clone repository
3. Set data to config.py
4. Create  virtual environments
5. Install packages in requirements.txt
6. import data to db - `python import_data_to_db.py`
7. run command `python main.py <id>`  example `python main.py 20`
