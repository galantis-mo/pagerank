DOCUMENTATION = """Python code to parse english wikilinks 'backup' from 2022-12-01
    # Input format
    | 1. When {-tl SIZE} is used, {-l LIMIT} should indicate the size of uris.txt and children.txt alltogether in mega bytes.
    | 2. When {-tl TRIPLE} is used, {-l LIMIT} should indicate the number of triples considered from the original file.

    # Output format
    | If -csv or --to_csv used, then only one file is created with parent; children
    | Otherwise should be saved in ~/pagerank/data/uris.txt and ~/pagerank/data/children.txt being respectively the list of all 
    | subjects and for each of them, all of their children.
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
import argparse, textwrap, math

def save_to_csv(decompressor, csv_path:str, limit:int, comp_space):
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
                    current_space_taken += comp_space(elements[0], False)
                else:
                    file_csv.write(' ')
                
                file_csv.write(elements[2])

                current_space_taken += comp_space(elements[2], True)
            
            # Uncomplete triple
            else:
                data = triple

    file_csv.write('"')
    file_csv.close()
    return current_space_taken


def DataFetcher(limit:int, is_limit_size:bool, save_comp:bool):
    import sys, os.path, bz2    # To manage files
    import urllib.request       # To fetch file
    import subprocess           # To execute linux commands

    # Paths to important files
    dir_path = os.path.dirname(__file__)
    original_path = os.path.join(dir_path, '..', 'data', 'original.bz2')
    output_csv_path = os.path.join(dir_path, '..', 'data', 'wikilinks.csv')

    print("Will try to save to", output_csv_path)

    # Fetch compressed data
    if os.path.exists(original_path):
        print("Original bz2 found file already existent in pagerank/data...")
        with open(original_path, 'rb') as f:
            compressed_data = f.read()

    else:
        request = urllib.request.Request("https://downloads.dbpedia.org/repo/dbpedia/generic/wikilinks/2022.12.01/wikilinks_lang=en.ttl.bz2")
        try:
            with urllib.request.urlopen(request) as data:
                print("Sending request to \"https://downloads.dbpedia.org/repo/dbpedia/generic/wikilinks/2022.12.01/wikilinks_lang=en.ttl.bz2\" ...")
                compressed_data = data.read()

        except urllib.error.URLError as e:
            print(e.reason)
            exit(1)

        # Using only 10% of the original file
        compressed_data = compressed_data[:math.ceil(len(compressed_data)*0.1)]
        if save_comp:
            with open(original_path, 'wb') as f:
                f.write(compressed_data)


    # Decompressing the zipped data
    print("Starting decompressing data from the original file")
    decompressor = bz2.BZ2Decompressor()
    decompressor.decompress(compressed_data, max_length=0).decode('utf-8', "backslashreplace")

    ## Limit size computation
    if is_limit_size:
        comp_space = lambda elm,_: len(elm)
    else:
        comp_space = lambda _,is_child: 1 if is_child else 0

    ## Output file
    current_space_taken = save_to_csv(decompressor, output_csv_path, limit, comp_space)

    ## Final report
    print("All triples extracted...") if decompressor.needs_input else print("Only some of triples were extracted...")    
    print(f"Output of size {current_space_taken/1000000} MB.") if is_limit_size else print(f"{current_space_taken} triples considered during decompression.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='python data_fetcher.py',
                    description=textwrap.dedent(DOCUMENTATION),
                    formatter_class=argparse.RawDescriptionHelpFormatter,
                    epilog='Monique RIMBERT (monique.rimbert@etu.univ-nantes.fr)')

    parser.add_argument('-tl', '--type_limit', type=str, choices=['SIZE', 'TRIPLE'], default="SIZE", help="The type of limit considered use 'SIZE' in MB and 'TRIPLE' as a int.")
    parser.add_argument('-l', '--limit', type=int, default=1, help='The size limit in type_limit of file.')
    parser.add_argument('-S', '--save_comp', action="store_true" , help="Saves the zip in data with to path ~/data/original.bz2")
    # parser.add_argument('--by_pair', action="store_true" , help="Format results in pair of parent-child")

    
    args = parser.parse_args()   
    DataFetcher(args.limit*1000000, args.type_limit == "SIZE", args.save_comp)