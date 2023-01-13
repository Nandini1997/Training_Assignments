def test_schema(tranformed_df, expected_df):
    field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
    fields1 = [*map(field_list, tranformed_df.schema.fields)]
    fields2 = [*map(field_list, expected_df.schema.fields)]
    res = set(fields1) == set(fields2)
    return res

def test_data(tranformed_df, expected_df):
    data1 = tranformed_df.collect()
    data2 = expected_df.collect()
    return set(data1) == set(data2)

