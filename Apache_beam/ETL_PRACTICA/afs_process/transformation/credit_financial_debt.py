def contar_cuentas(element):
    cuentas = []
    for i in element.get('details', []):
        cuentas.append(i.get('account_number'))
    return len(list(set(cuentas)))

def create_financial_vars(element, date, valor_uf):
    # Ejemplo de cálculos financieros
    return {"some_key": "value"}
