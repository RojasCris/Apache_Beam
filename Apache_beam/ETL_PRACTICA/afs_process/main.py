import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

import configure.configure as C
import query.query as Q
import modules.financial as MCFD
import modules.module_checking as chk
import modules.module_moneda as MON
import transformation.process as prs

def add_moneda_to_credit(credit_element, moneda_value):
    rut, data = credit_element
    data.update(moneda_value)
    return rut, data

class MisOpciones(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--archive', help='Fecha de partition output', required=True)
        parser.add_argument('--process_date', help='Fecha de cálculo', required=True)
        parser.add_argument('--environment', help='Ambiente de ejecución', required=True)
        parser.add_argument('--projectid', help='Proyecto ID', required=True)

def proceso(pipeline_options, process_date, archive):
    query_rutero = Q.sql_query_rutero(C.projectid, C.dbase, C.identity, C.comercial_identity, process_date)

    with beam.Pipeline(options=pipeline_options) as p:
        pcoll_moneda = (
            p | "procesar moneda" >> MON.module_moneda(p, process_date)
        )

        rutero = (
            p | "Leer rutero desde BigQuery" >> beam.io.ReadFromBigQuery(query_rutero, use_standard_sql=True)
              | "Map rutero" >> beam.Map(U.map_to_pair, 'rut')
              | "Agregar moneda" >> beam.Map(add_moneda_to_credit, beam.pvalue.AsSingleton(pcoll_moneda))
        )

        credit_financial_debt_uat_value = (
            p | "Procesar crédito" >> MCFD.credit_financial_debt_value(p, process_date)
              | "Map crédito" >> beam.Map(U.map_to_pair, 'rut')
        )

        checking = (
            p | "Procesar checking" >> chk.checking_module(p, process_date)
              | "Map checking" >> beam.Map(U.map_to_pair, 'owner_rut')
        )

        _join = (
            {"rutero": rutero, "credit": credit_financial_debt_uat_value, "checking": checking}
            | "Merge" >> beam.CoGroupByKey()
            | "Filtrar por RUT" >> beam.Filter(lambda x: len(x[1]['rutero']) > 0)
            | "Map final" >> beam.Map(lambda x: {'rutero': x[1]['rutero'][0], ...})
            | "Crear AFS" >> beam.ParDo(prs.create_afs(), process_date, archive)
        )

        writer_output_attobs_pcoll = (
            _join | "Escribir en BigQuery" >> beam.io.WriteToBigQuery(
                "{}:{}.{}".format(C.projectid, C.dataset, "cl_attribute_afs"),
                schema=C.schema_afs,
                additional_bq_parameters={'timePartitioning': {'type': 'DAY', 'field': 'archive', 'requirePartitionFilter': True}}
            )
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    args = MisOpciones()
    process_date = args.process_date
    archive = args.archive
    projectid = args.projectid
    environment = args.environment

    proceso(pipeline_options, process_date, archive)
