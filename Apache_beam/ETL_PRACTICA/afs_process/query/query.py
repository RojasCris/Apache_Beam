def qry_credit_financial_debt(table, projectid, sandbox, process_date):
    qry = """
    SELECT ...
    FROM `{projectid}.{sandbox}.{table}`
    WHERE archive IS NOT NULL
    """.format(projectid=projectid, sandbox=sandbox, table=table)
    return qry

# Similar lógica para otras consultas
