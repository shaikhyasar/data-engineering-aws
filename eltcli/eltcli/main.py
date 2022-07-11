#!/usr/bin/env python
import typer
import sqlite3
import pandas as pd
from os.path import splitdrive
app = typer.Typer()

@app.command()
def extract(
    database = typer.Option(...,help="Give the path of the database"),
    sql = typer.Option(...,help="Enter the sql"),
    params = typer.Option(...,help="provide the paramters"),
    target = typer.Option(...,help="enter the target location"),
    ):
    """
    Extract command is used to extract the data from the database and save it as CSV to requested location
    """
    key, value = params.replace("{","").replace("}","").split(":")
    value = "'%" + value.replace("'","")  + "%'"
    con = sqlite3.connect(database)
    with open(sql) as f:
        query = f.read()
    query += f" WHERE datetime LIKE {value}"
    df= pd.read_sql(query,con)
    df.to_csv(target, index=False)
    typer.echo(f"The file is saved on {target}")

@app.command()
def load(
    input = typer.Option(...,help="Give the path of the csv file"),
    database = typer.Option(...,help="Give the path of the database"),
    target = typer.Option(...,help="enter the target table"),
):
    """
    Load command is used to extract the data from the CSV and save it to database in the requested table
    """
    con = sqlite3.connect(database)
    df= pd.read_csv(input)
    df['datetime'] = pd.to_datetime(df['datetime'], format="%Y-%m-%dT%H:%M:%S.%f%z")
    df.to_sql(target,con=con,if_exists='replace')
    
    typer.echo("The data are loaded successfully")

@app.command()
def transform(
    database = typer.Option(...,help="Give the path of the database"),
    sql = typer.Option(...,help="Enter the sql"),
    params = typer.Option(...,help="provide the paramters"),
    target = typer.Option(...,help="enter the target table"),
):
    """
    transform command is used to perform some transformation on the loaded table
    """
    key, value = params.replace("{","").replace("}","").split(":")
    value = "'%" + value.replace("'","")  + "%'"
    table_one,table_two = target.split("_")
    con = sqlite3.connect(database)
    f = open(sql)
    query = f.readline()
    query += f" WHERE datetime LIKE {value} GROUP BY customer_id, DATE(datetime) ORDER BY DATE(datetime)"
    df = pd.read_sql(query,con)
    df.to_sql(table_one,con,if_exists='replace', index=False)
    query2 = f.readline()
    df1 = pd.read_sql(query2,con)
    df1.to_sql(table_two,con,if_exists='replace',index=False)
    f.close()

    typer.echo("Transformation processed completed")


app()

