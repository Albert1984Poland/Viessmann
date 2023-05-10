# COMMAND ----------
from viessmann_lib.file_processor import FileProcessor

dbutils.widgets.text("source_path", '')
dbutils.widgets.text("destination_path", '')

# COMMAND ----------
src_path = dbutils.widgets.get("source_path")
dst_path = dbutils.widgets.get("destination_path")

FileProcessor(src_path=src_path,
              dst_path=dst_path,
              spark_session=spark).run()