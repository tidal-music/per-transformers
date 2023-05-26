# from transformers.main_artist_compound_mapping_transformer import MainArtistCompoundMappingTransformer
# from pyspark_test import PySparkTest
# from pyspark_commons_test.test_datasets import get_awm_artist_compound_mapping, get_csv_playback_logs
#
#
# class MainArtistCompoundTransformerTest(PySparkTest):
#
#     def test_main_artist_compound_transformer(self):
#         mapping = get_awm_artist_compound_mapping(self.sc)
#         dataset = get_csv_playback_logs(self.sc).select("logEntryUuid", "artistId")
#
#         output = MainArtistCompoundMappingTransformer(mapping).transform(dataset)
#
#         self.assertEqual(len(dataset.columns), len(output.columns))
#         self.assertEqual(output.count(), dataset.count())
#         self.assertEqual(output.select("logEntryUuid").distinct().count(), dataset.select("logEntryUuid").distinct().count())
#
#         # Check that an artist with a mapping is correctly mapped
#         mapped_log_entry = "614a0a7e-e02d-432d-96d3-a15e825f4534"
#         self.assertEqual(dataset.where(f"logEntryUuid = '{mapped_log_entry}'").collect()[0]["artistId"], '6083314')
#         self.assertEqual(output.where(f"logEntryUuid = '{mapped_log_entry}'").collect()[0]["artistId"], '24995')
#
#         # Check that a artist with no mapping is not mapped
#         non_mapped_log_entry = "f89fc198-65e9-445d-ba96-d1d99ec22224"
#         self.assertEqual(dataset.where(f"logEntryUuid = '{non_mapped_log_entry}'").collect()[0]["artistId"], '3702744')
#         self.assertEqual(output.where(f"logEntryUuid = '{non_mapped_log_entry}'").collect()[0]["artistId"], '3702744')
#
#
