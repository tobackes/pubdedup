import numpy as np
import gmpy2

def stream_bits(strings):
    stream = [np.frombuffer(bytes(string,'ascii'),dtype=np.uint8) for string in strings];
    maxlen = max((len(L) for L in stream));
    for i in range(maxlen):
        indices = [];
        barray  = [];
        for j in range(len(stream)):
            if i < len(stream[j]):
                indices.append(j);
                barray.append(stream[j][i]);
        yield np.array(indices), np.array(barray);

def fnv1a(features): # This seems to be correct, although the similarity of similar features is not reflected
    p = np.array([1099511628211],dtype=np.uint64); # This is an array to avoid overflow checks and warning
    h = np.repeat(14695981039346656037,len(features));
    for indices,Bytes in stream_bits(features):
        h[indices] ^= Bytes;#np.left_shift(np.right_shift(h[indices],8),8) + Bytes;
        h[indices] *= p;
    return h;

def dif(hashes,i,j):
    return gmpy2.popcount(int(hashes[i]^hashes[j]));

def feats2docint(features):
    hashes   = fnv1a(features);
    booles   = np.unpackbits(hashes.view(np.uint8),bitorder='little').reshape(len(hashes),64)[:,::-1];
    avg_bool = booles.sum(0)/len(booles)>=0.5;
    avg_int  = np.packbits(avg_bool.reshape(8, 8)[::-1]).view(np.uint64);
    return avg_int;


features = ['this is a test','test','this is a','this is a test','this is test','this is tes'];

