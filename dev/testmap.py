import pandas as pd
import numpy as np
from mapper import HashDict


if __name__ == "__main__":
    hashdict = HashDict(8, 'test')

    numpoints = 1000
    df = pd.DataFrame(data=np.arange(numpoints), columns=['numbers'])
    print(df)

    b = df.numbers.map(hashdict)
    print(b)
    print(hashdict)
