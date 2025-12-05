import os
import sys

PROJECT_ID = os.environ['PROJECT_ID']
BUCKET_NAME = os.environ['BUCKET']

def save_to_csv(decompressor, csv_path, limit):
    # Open the file in write mode
    file_csv = csv_path.open(mode="w", encoding="utf-8")
    file_csv.write('"";"')

    # Decompressed data and tracking current parent
    data = ""
    current_space_taken = 0
    current_parent = ""

    while (not decompressor.needs_input) and (current_space_taken < limit):
        data += decompressor.decompress(b'', max_length=256).decode('utf-8', "backslashreplace")

        # Reading all triples in buffer
        for triple in data.split('\n'):

            # Check for complete triple
            if triple.endswith(' .'):
                elements = triple.split(' ')

                if elements[0] != current_parent:
                    file_csv.write('"\n"{}";"'.format(elements[0]))
                    current_parent = elements[0]
                    current_space_taken += len(elements[0])
                else:
                    file_csv.write(' ')
                
                file_csv.write(elements[2])

                current_space_taken += len(elements[2])
            
            # Uncomplete triple
            else:
                data = triple

    file_csv.write('"')
    file_csv.close()
    return current_space_taken

def DataFetcher(limit):
    from google.cloud import storage
    import bz2    # To manage files
    import urllib.request       # To fetch file

    # Fetch compressed data
    request = urllib.request.Request("https://downloads.dbpedia.org/repo/dbpedia/generic/wikilinks/2022.12.01/wikilinks_lang=en.ttl.bz2")
    try:
        with urllib.request.urlopen(request) as data:
            print("Sending request to \"https://downloads.dbpedia.org/repo/dbpedia/generic/wikilinks/2022.12.01/wikilinks_lang=en.ttl.bz2\" ...")
            compressed_data = data.read(180000000)

    except urllib.error.URLError as e:
        print("Fail to request or read wikilinks_lang=en.ttl.bz2:", e.reason)
        exit(1)

    # Paths to important files
    storage_client = storage.Client(PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob("data/wikilinks_full.csv")
    
    print("Will try to save to", BUCKET_NAME)

    # Decompressing the zipped data
    print("Starting decompressing data from the original file")
    decompressor = bz2.BZ2Decompressor()
    decompressor.decompress(compressed_data, max_length=0).decode('utf-8', "backslashreplace")

    current_space_taken = save_to_csv(decompressor, blob, limit)

    ## Final report
    print("All triples extracted...") if decompressor.needs_input else print("Only some of triples were extracted...")    
    print(f"Output of size {current_space_taken/1000000} MB.")

if __name__ == '__main__':
    if len(sys.argv) < 2 or not sys.argv[1].isdigit():
        print("L'entièreté du fichier sera décompressé !")
        DataFetcher(float('inf'))
    else:
        # Appel de la méthode
        DataFetcher(int(sys.argv[1])*1000000)











