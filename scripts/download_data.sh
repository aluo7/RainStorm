#!/bin/bash

wget -P data/ "https://snap.stanford.edu/data/gplus.tar.gz";
wget -P data/ "https://www.cs.upc.edu/~nlp/wikicorpus/raw.en.tgz";

tar -xzf gplus.tar.gz;

mkdir wikicorpus;
tar -xzf raw.en.tgz -C wikicorpus;