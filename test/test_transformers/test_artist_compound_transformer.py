# from unittest import mock
# from transformers.artist_compound_transformer import ArtistCompoundMappingTransformer
# from pyspark_test import PySparkTest
# from pyspark_commons_test.test_datasets import get_awm_artist_compound_mapping, get_csv_playback_logs
# import utils.constants as c
#
#
# class ArtistCompoundTransformerTest(PySparkTest):
#
#     def test_artist_compound_transformer(self):
#         mapping = get_awm_artist_compound_mapping(self.sc)
#         dataset = get_csv_playback_logs(self.sc)
#
#         output = ArtistCompoundMappingTransformer(4, compound_artist_mapping=mapping).transform(dataset)
#
#         assert(len(dataset.columns) == len(output.columns))
#         assert(output.count() > dataset.count())
#
#         # Check that an artist with a mapping is correctly mapped
#         mapped_log_entry = "614a0a7e-e02d-432d-96d3-a15e825f4534"
#         assert (dataset.where(f"logEntryUuid = '{mapped_log_entry}'").collect()[0]["artistId"] == '6083314')
#         assert (output.where(f"logEntryUuid = '{mapped_log_entry}'").count() == 2)
#
#         # Check that a artist with no mapping is not mapped
#         non_mapped_log_entry = "f89fc198-65e9-445d-ba96-d1d99ec22224"
#         assert (dataset.where(f"logEntryUuid = '{non_mapped_log_entry}'").collect()[0]["artistId"] == '3702744')
#         assert (output.where(f"logEntryUuid = '{non_mapped_log_entry}'").collect()[0]["artistId"] == '3702744')
#
#         output = ArtistCompoundMappingTransformer(4, keep_info=True).transform(dataset)
#         assert (len(dataset.columns) + 2 == len(output.columns))
#         assert c.MAIN_ARTIST in output.columns
#         assert c.PRIORITY in output.columns
