python eltcli.py extract --database source.db --sql query.sql --params '{"date":"2011-12-08"}' --target transactions_2011-12-08.csv
python eltcli.py load --input transactions_2011-12-08.csv --database base.db --target transactions
python eltcli.py transform --database base.db --sql aggregation-user-day.sql --params '{"date":"2011-12-08"}'  --target transactions_daily
