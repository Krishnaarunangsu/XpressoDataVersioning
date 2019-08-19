import pickle

a = {'hello': 'world'}

with open('filename2.pickle', 'ab') as handle:
    pickle.dump(a, handle)

