import os
import glob
import re

DATA_DIR = "./data/gplus/"
OUT_EXP1 = "./data/exp1_input.csv"
OUT_EXP2 = "./data/exp2_input.csv"

def read_featnames(fname):
    """
    Reads a featnames file containing lines: <index> <featureName>
    Returns a dict index -> featureName.
    """
    featnames = {}
    with open(fname, "r") as f:
        for line in f:
            idx, name = line.strip().split(" ", 1)
            featnames[int(idx)] = name
    return featnames

def clean_featnames(names):
    """
    Cleans feature names leaving only A-z0-9:_-
    """
    return {k: re.sub(r'[^A-Za-z0-9:_-]', '', v) for k, v in names.items()}



def read_feats(fname):
    """
    Reads feat vector lines: <featVal0> <featVal1> ...
    Returns list of indices with value == 1.
    """
    with open(fname, "r") as f:
        vec = list(map(int, f.read().strip().split()))
        return [i for i,v in enumerate(vec) if v == 1]
        # return [i for i,v in enumerate(vec)]

def main():
    exp1 = open(OUT_EXP1, "w")
    exp2 = open(OUT_EXP2, "w")

    featnames_files = glob.glob(os.path.join(DATA_DIR, "*.featnames"))

    print(len(featnames_files))

    for i in range(100):

        for fn_file in featnames_files:
            userid = os.path.basename(fn_file).split(".")[0]
            feat_file = fn_file.replace("featnames", "feat")

            if not os.path.exists(feat_file):
                continue

            names = clean_featnames(read_featnames(fn_file))
            active_indices = read_feats(feat_file)

            active_feats = [names[i] for i in active_indices if i in names]

            print(len(active_feats))

            # EXPERIMENT 1
            # Format: userid,feat1|feat2|feat3
            exp1.write(f"{userid},{'|'.join(active_feats)}\n"*100)

            # EXPERIMENT 2
            # Format: featname
            for feat in active_feats:
                exp2.write(f"{feat}\n")

    exp1.close()
    exp2.close()
    print("Generated ", OUT_EXP1, OUT_EXP2)

if __name__ == "__main__":
    main()
