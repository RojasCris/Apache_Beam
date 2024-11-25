class create_afs(beam.DoFn):
    def process(self, record, p_date, archive):
        records = {}
        # Procesamiento de registros
        yield records
