import os
import re
import csv

INPUT_DIR = "./data/wikicorpus"
OUTPUT_DIR = "./data/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

DOC_START_RE = re.compile(r'<doc id="(\d+)"[^>]*>')
DOC_END_RE = re.compile(r'</doc>')

out_filename = "exp3_input"
output_path = os.path.join(OUTPUT_DIR, out_filename + ".csv")


with open(output_path, "w", newline="", encoding="utf-8") as f_out:

    for filename in os.listdir(INPUT_DIR):
        if not filename.startswith("englishText_"):
            continue

        input_path = os.path.join(INPUT_DIR, filename)



        with open(input_path, "r", encoding="utf-8", errors="ignore") as f_in:

            writer = csv.writer(f_out)
            
            doc_id = None
            text_lines = []

            for line in f_in:
                line = line.strip()
                start_match = DOC_START_RE.match(line)
                if start_match:
                    doc_id = start_match.group(1)
                    text_lines = []
                    continue

                if DOC_END_RE.match(line):
                    if doc_id is not None:
                        text = " ".join(text_lines)
                        writer.writerow([doc_id, text])
                    doc_id = None
                    text_lines = []
                    continue

                if doc_id is not None:
                    text_lines.append(line)

        print(f"Processed {input_path} -> {output_path}")

print("All files processed.")
