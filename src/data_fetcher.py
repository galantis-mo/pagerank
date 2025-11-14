DOCUMENTATION = """Python code to parse english wikilinks 'backup' from 2022-12-01
    # Input format
    | 1. When {-tl SIZE} is used, {-l LIMIT} should indicate the size of uris.txt and children.txt alltogether in mega bytes.
    | 2. When {-tl TRIPLE} is used, {-l LIMIT} should indicate the number of triples considered from the original file.

    # Output format
    | Outputs should be saved in ~/pagerank/data/uris.txt and ~/pagerank/data/children.txt being respectively the list of all 
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
import argparse, textwrap

def DataFetcher(limit:int, is_limit_size:bool, save_comp:bool):
    import sys, os.path, bz2    # To manage files
    import urllib.request       # To fetch file
    import subprocess           # To execute linux commands

    # Paths to important files
    dir_path = os.path.dirname(os.path.abspath(__file__))
    data_dir_path = os.path.join(os.path.dirname(dir_path), 'data')

    original_path = os.path.join(data_dir_path, 'original.bz2')
    childrens_path = os.path.join(data_dir_path, 'children.txt')
    uris_path = os.path.join(data_dir_path, 'uris.txt')

    # Fetch compressed data
    if os.path.exists(original_path):
        print("Original bz2 file already existent in pagerank/data...")
        with open(original_path, 'rb') as f:
            compressed_data = f.read()
    else:
        request = urllib.request.Request("https://downloads.dbpedia.org/repo/dbpedia/generic/wikilinks/2022.12.01/wikilinks_lang=en.ttl.bz2")
        try:
            with urllib.request.urlopen(request) as data:
                print("Sending request to \"https://downloads.dbpedia.org/repo/dbpedia/generic/wikilinks/2022.12.01/wikilinks_lang=en.ttl.bz2\" ...")

                compressed_data = data.read()
                
                # Save the original bz2 file
                if save_comp:
                    with open(original_path, 'wb') as f:
                        f.write(compressed_data)
        except urllib.error.URLError as e:
            print(e.reason)

    # Open files in write mode
    file_children = open(childrens_path, "w")
    file_uris = open(uris_path, "w")
    file_uris.write('\n')

    current_space_taken = 0

    # Decompressing the zipped data
    print("Decompressing data from the original file")
    decompressor = bz2.BZ2Decompressor()
    unzipped_data = decompressor.decompress(compressed_data, max_length=256).decode('utf-8', "backslashreplace")
    current_id = ""

    print(f"Limit set to {limit} MB = {limit*1000000} B")
    limit = 1000000
    while (current_space_taken < limit):
        unzipped_data += decompressor.decompress(b'', max_length=256).decode('utf-8', "backslashreplace")
        
        for triple in unzipped_data.split('\n'):       
            if triple.endswith(' .'):   # Triples follows format : <sub> <pred> <obj> .
                elms = triple.split(' ')

                # New subject
                if elms[0] != current_id:
                    subject = elms[0]+'\n'
                    file_uris.write(subject)
                    file_children.write('\n')

                    current_id = elms[0]
                    current_space_taken += len(subject) if is_limit_size else 0
                
                # New child
                if len(elms) < 2:
                    print(elms)
                child = elms[-2] + ' '
                file_children.write(child)
                current_space_taken += len(child) if is_limit_size else 1

            else:
                # Not complete triple
                unzipped_data = triple
    
    # Final report
    file_children.close()
    file_uris.close()

    if decompressor.needs_input:
        print("All triples extracted...")
    else:
        print("Only some of triples were extracted...")

    if is_limit_size:
        print(f"Output of size {current_space_taken} MB.")
    else:
        print(f"{current_space_taken} triples considered during decompression.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
                    prog='data_fetcher.py',
                    description=textwrap.dedent(DOCUMENTATION),
                    formatter_class=argparse.RawDescriptionHelpFormatter,
                    epilog='Monique RIMBERT (monique.rimbert@etu.univ-nantes.fr)')

    parser.add_argument('-tl', '--type_limit', type=str, choices=['SIZE', 'TRIPLE'], default="SIZE", help="The type of limit considered use 'SIZE' in MB and 'TRIPLE' as a int.")
    parser.add_argument('-l', '--limit', type=int, default=1000, help='The size limit in type_limit of file.')
    parser.add_argument('-S', '--save_comp', action="store_true" , help="Saves the zip in data with to path ~/data/original.bz2")
    
    args = parser.parse_args()   
    DataFetcher(args.limit, args.type_limit == "SIZE", args.save_comp)