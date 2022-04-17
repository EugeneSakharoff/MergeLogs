from pathlib import Path
from contextlib import contextmanager
import argparse
from tqdm import tqdm

#Creating a context manager object to open several files in different modes simultaniously using with...as...:
@contextmanager
def open_files(*args):
    if args:
        #arg[0] is Path object, arg[1] is a mode ("r","w", etc.)
        files = {arg[0].stem:arg[0].open(arg[1]) for arg in args}
        try:
            yield files
        finally:
            for f in files:
                files[f].close()

#Main function recieves exactly one output file and arbitrary number of input files
def merge_logs(output,*args):
    sumsize = sum([f.stat().st_size for f in args])
    pbar = tqdm(total = sumsize) #progress bar
    args = [(arg,"rb") for arg in args] #adding "rb" mode to every file except for the first one
    with open_files((output,"wb"),*args) as files:
        output_name=output.stem  #'files' is a dictionary so 'output_name' is an output file's key  
        current_lines={}
        for fname in set(files.keys())-{output_name}: #set of input file names
            current_lines[fname]=files[fname].readline() #first line in every input file
            if not current_lines[fname]:    #if file is empty
                current_lines.pop(fname)    #we are not processing it anymore
        #Sorting timestamps to achieve best performance within 'while' loop
        #I am using substring search since it is faster than deserializing json
        #also, I am comparing strings straightaway because it is faster than converting string into datetime or anything else and then comparing
        #!!WARNING This will only work in python>= 3.6 since dicts were unordered in older versions!!
        timestamps={key:value.split(b'amp": "')[1].split(b'",')[0] for key,value in sorted(current_lines.items(), key=lambda item: item[1])} 
        
        while current_lines.keys():  #while there are unprocessed lines          
            f_min=list(timestamps.keys())[0]
            files[output_name].write(current_lines[f_min])     #writing output
            pbar.update(len(current_lines[f_min]))
            current_lines[f_min] = files[f_min].readline() #reading next line in certain file
            if not current_lines[f_min]:                       #if file is over 
                current_lines.pop(f_min)                       #we are not processing it anymore
                timestamps.pop(f_min)
            else:
                timestamps[f_min] = current_lines[f_min].split(b'amp": "')[1].split(b'",')[0]  #getting timestamp from new line
                timestamps={key:value for key,value in sorted(timestamps.items(), key=lambda item: item[1])}  #this should be fast as only single element is unsorted
        pbar.close()
                        
 
def main():
    parser = argparse.ArgumentParser(description='Merges arbitrary number of sorted *.json files into one')
    parser.add_argument('input1', type=str, help='Input file 1')
    parser.add_argument('input2', type=str, help='Input file 2')
    parser.add_argument('-o','--output', type=str, help='Output file')
    parser.add_argument('add_inputs', nargs='*')

    args = parser.parse_args()
    inputs =[Path(args.input1),Path(args.input2)]
    out=Path(args.output)
    for i in args.add_inputs:
        inputs.append(Path(i))
        
    for i in inputs:
        if not i.exists(): 
            print("Error: file {} not found".format(i.resolve()))
            return   
        
    if not(out.exists()):
        out.parent.mkdir()
        out.touch()
    else:
        out.unlink()
        out.touch()
    merge_logs(out,*inputs)


if __name__ == '__main__':
    main()               
 

   
    
  