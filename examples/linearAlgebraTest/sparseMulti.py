
import sys
import numpy as np
import scipy.sparse as spp

#row, column, value
dtype = np.dtype([('row',np.int64),('column',np.int64), ('data',np.int64)])

def get_data(filename):
    table = np.loadtxt(filename, dtype, delimiter=',')
    rows = table['row']
    cols = table['column']
    data = table['data']
    max_dimension = max(np.max(cols),np.max(rows)) + 1
    return (data,(rows,cols)), max_dimension

def main():
    outfile = sys.argv[3]
    data_1, size_1 = get_data(sys.argv[1])
    data_2, size_2 = get_data(sys.argv[2])
    max_shape = (max(size_1,size_2),)*2
    matrix_1 = spp.csr_matrix(spp.coo_matrix(data_1,dtype=np.int64,shape=max_shape))
    matrix_2 = spp.csr_matrix(spp.coo_matrix(data_2,dtype=np.int64,shape=max_shape))
    result = matrix_1*matrix_2
    with open(outfile, 'w') as f:
        rows, cols = result.nonzero()
        for i in xrange(rows.shape[0]):
            row = rows[i]
            col = cols[i]
            val = result[row,col]
            out_string = "{},{},{}\n".format(row,col,val)
            f.write(out_string)

if __name__ == "__main__":
    main()
