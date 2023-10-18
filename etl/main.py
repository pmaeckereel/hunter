import sys
import logging

from ingester import KaggleIngester

if __name__ == "__main__":
	allowed_args = ["crunchbase", "kaggle", "hunter"]
	if len(sys.argv) == 1:
		logging.error("Error. Require at least one argument: the path to the directory containing the source datasets. Exiting...")
		sys.exit(1)
	elif len(sys.argv) == 2:
		## Ingest all 3 data sources into psql.
		sources_to_ingest = allowed_args
	else:
		if sys.argv[2].lower() not in allowed_args:
			logging.error(f"Error. If a second argument is provided, it must be one of '{', '.join(allowed_args)}'. Exiting...")
			sys.exit(1)
		else:
			sources_to_ingest = [sys.argv[2].lower()]

	datasets_path = sys.argv[1]

	for source in sources_to_ingest:
		if source == "kaggle":
			ingester = KaggleIngester(datasets_path + "Fortune_500_Kaggle.csv")
		elif source == "crunchbase":
			ingester = CrunchbaseIngester(datasets_path + "Fortune_500_Crunchbase.csv")
		else:
			ingester = HunterIngester(datasets_path + "Fortune_500_Hunter.csv")
		ingester.load_data()
		tables_data = ingester.prepare_table_data()
		ingester.ingest_data(tables_data)