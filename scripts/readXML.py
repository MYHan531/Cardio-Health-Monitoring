import xml.etree.ElementTree as ET
import csv
import os

# === CONFIGURATION ===
INPUT_XML_FILE = "../data/raw/apple_health_export/export.xml"
OUTPUT_CSV_FILE = "../data/raw/parsed_export.csv"

# === METRIC TYPES TO EXTRACT ===
CARDIO_METRICS = {
    "HKQuantityTypeIdentifierRestingHeartRate",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN",
    "HKQuantityTypeIdentifierVO2Max",
    "HKQuantityTypeIdentifierWalkingHeartRateAverage",
    "HKQuantityTypeIdentifierDistanceWalkingRunning",
    "HKQuantityTypeIdentifierActiveEnergyBurned",
    "HKQuantityTypeIdentifierFlightsClimbed",
    "HKQuantityTypeIdentifierHeartRate",
}

# === PARSER FUNCTION ===
def parse_large_xml(input_file, output_csv):
    context = ET.iterparse(input_file, events=("start", "end"))
    _, root = next(context)  # get root of XML

    with open(output_csv, "w", newline="") as csvfile:
        fieldnames = ["type", "value", "unit", "startDate", "endDate", "sourceName"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for event, elem in context:
            if event == "end" and elem.tag == "Record":
                record_type = elem.attrib.get("type")

                if record_type in CARDIO_METRICS:
                    writer.writerow({
                        "type": record_type,
                        "value": elem.attrib.get("value"),
                        "unit": elem.attrib.get("unit"),
                        "startDate": elem.attrib.get("startDate"),
                        "endDate": elem.attrib.get("endDate"),
                        "sourceName": elem.attrib.get("sourceName")
                    })

                elem.clear()
                root.clear()

    print(f"\n✅ Finished parsing. CSV written to: {os.path.abspath(output_csv)}")

# === MAIN ===
if __name__ == "__main__":
    if not os.path.exists(INPUT_XML_FILE):
        print(f"❌ File not found: {INPUT_XML_FILE}")
    else:
        parse_large_xml(INPUT_XML_FILE, OUTPUT_CSV_FILE)

