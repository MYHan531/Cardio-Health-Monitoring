import logging
from pathlib import Path
from typing import Iterator, Dict, Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_health_records(xml_path: str, batch_size: int = 100_000) -> Iterator[Dict[str, Any]]:
    """
    Parse Apple Health XML records in batches.
    
    Args:
        xml_path: Path to the Apple Health export.xml file
        batch_size: Number of records to process in each batch
        
    Yields:
        Dictionary containing batch of health records
    """
    try:
        context = etree.iterparse(xml_path, events=('end',), tag='Record')
        records = []
        expected_columns = ['type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 'startDate', 'endDate', 'value', 'device']
        
        for _, elem in context:
            # Explicitly convert .attrib to a dictionary
            record = dict(elem.attrib)
            # Ensure all expected columns are present, fill missing ones with None
            for col in expected_columns:
                if col not in record:
                    record[col] = None
            records.append(record)
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
                
            if len(records) >= batch_size:
                yield records
                records.clear()
                
        if records:  # Yield any remaining records
            yield records
            
    except Exception as e:
        logger.error(f"Error parsing XML file: {e}")
        raise

def convert_to_parquet(xml_path: str, output_path: str, batch_size: int = 100_000) -> None:
    """
    Convert Apple Health XML export to Parquet format.
    
    Args:
        xml_path: Path to the Apple Health export.xml file
        output_path: Path where the parquet file will be saved
        batch_size: Number of records to process in each batch
    """
    try:
        # Create output directory if it doesn't exist
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # First, read all records to get the schema
        logger.info("Reading first batch to determine schema...")
        first_batch = next(parse_health_records(xml_path, batch_size))
        df = pd.DataFrame(first_batch)
        logger.info(f"First batch length: {len(df)}")
        logger.info(f"First batch columns: {df.columns.tolist()}")
        logger.info(f"First batch sample:\n{df.head()}")
        
        # Explicitly reorder columns to match the existing Parquet file schema
        expected_columns = ['type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 'startDate', 'endDate', 'value', 'device']
        df = df[expected_columns]
        
        schema = pa.Table.from_pandas(df).schema
        
        # Create a temporary file for writing
        temp_path = str(Path(output_path).with_suffix('.tmp.parquet'))
        
        # Write all batches to the temporary file
        total_records = 0
        with pq.ParquetWriter(
            temp_path,
            schema,
            compression='zstd',
            version='2.6'
        ) as writer:
            # Write the first batch
            writer.write_table(pa.Table.from_pandas(df))
            total_records += len(first_batch)
            logger.info(f"Processed {total_records} records")
            
            # Write remaining batches
            for batch in parse_health_records(xml_path, batch_size):
                df = pd.DataFrame(batch)
                df = df[expected_columns]  # Reorder columns for each batch
                writer.write_table(pa.Table.from_pandas(df))
                total_records += len(batch)
                logger.info(f"Processed {total_records} records")
        
        # Replace the original file with the temporary file
        Path(temp_path).replace(output_path)
        logger.info(f"Successfully converted {total_records} records to {output_path}")
            
    except Exception as e:
        logger.error(f"Error converting to parquet: {e}")
        # Clean up temporary file if it exists
        if 'temp_path' in locals() and Path(temp_path).exists():
            Path(temp_path).unlink()
        raise

def print_first_record(xml_path: str):
    """Print the first <Record> element and its attributes for debugging."""
    try:
        context = etree.iterparse(xml_path, events=('end',), tag='Record')
        for _, elem in context:
            print("First <Record> element:")
            print(etree.tostring(elem, pretty_print=True).decode())
            print("Attributes:")
            print(elem.attrib)
            break
    except Exception as e:
        print(f"Error reading XML: {e}")

def main():
    """Main function to run the conversion process."""
    xml_path = "../data/raw/apple_health_export/export.xml"
    output_path = "../data/raw/apple_health_records.parquet"
    
    try:
        convert_to_parquet(xml_path, output_path)
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "debug-record":
        print_first_record("../data/raw/apple_health_export/export.xml")
    else:
        main()
