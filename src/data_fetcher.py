DOCUMENTATION = """Python code to parse english wikilinks 'backup' from 2022-12-01
    # Input format
    | 1. When {-l LIMIT} should indicate the size of uris.txt and children.txt alltogether in mega bytes.

    # Output format
    | A parent and the list of its children
    | Example :     uris.txt   |   children.txt
    |                <a>       |    <b> <c> <d>
    |                <b>       |    <d> <e>

    # Possible error during execution 
    | Sometimes `bz2 module is not available error` happens. In this case :
    | > cp /usr/lib/<PYTHON_VERSION>/lib-dynload/_bz2.cpython-<PYTHON_VERSION>-x86_64-linux-gnu.so /usr/local/lib/<PYTHON_VERSION>/
    | should suffice to fix it.

    # Implementation comment
    | Beware that opening solely this url will result in redirection, hence use urllib.request.Request to process redirections.
    | The unzipped file will be saved in ~/pagerank/data/original.bz2 if needed.
"""
import argparse, textwrap

def save_to_csv(decompressor, csv_path:str, limit:int):
    # Open the file in write mode
    file_csv = open(csv_path, "w")
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

def DataFetcher(limit:int, output_dir:str):
    import os.path, bz2    # To manage files
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
    output_csv_path = os.path.join(output_dir, 'wikilinks.csv')
    print("Will try to save to", output_csv_path)

    # Decompressing the zipped data
    print("Starting decompressing data from the original file")
    decompressor = bz2.BZ2Decompressor()
    decompressor.decompress(compressed_data, max_length=0).decode('utf-8', "backslashreplace")

    ## Output file
    current_space_taken = save_to_csv(decompressor, output_csv_path, limit)

    ## Final report
    print("All triples extracted...") if decompressor.needs_input else print("Only some of triples were extracted...")    
    print(f"Output of size {current_space_taken/1000000} MB.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='python data_fetcher.py',
                    description=textwrap.dedent(DOCUMENTATION),
                    formatter_class=argparse.RawDescriptionHelpFormatter,
                    epilog='')

    parser.add_argument('--limit', type=int, default=1, help='The size limit of file in MB.')
    parser.add_argument('--output_dir', help="The path of the output directory")

    args = parser.parse_args()   
    DataFetcher(args.limit*1000000, args.output_dir)