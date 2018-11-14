from mrjob.job import MRJob

# mongo clients libs
import happybase

# Generic imports
import glob
from json import load


class MRJob_delete_measures(MRJob):
    
    #INPUT_PROTOCOL = PickleValueProtocol
    
    def mapper_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))
        
    def reducer_init(self):
        # recover json configuration uploaded with script
        fn = glob.glob('*.json')
        self.config = load(open(fn[0]))
        
        # hbase connections
        self.hbase = happybase.Connection(self.config['hbase']['host'], self.config['hbase']['port'])
        self.hbase.open()
        
    def mapper(self, _, doc):   #we don't have value -> input protocol pickleValue which means no key is read
        
        doc = doc.split('\t')
        # doc generation
        d = {
             'key': doc[0],
             'table': doc[1],
            }
        
        yield d['table'], d['key']
        
    def reducer(self, key, values):
        
        # Delete the contracts in Hbase using a batch process
        table = self.hbase.table(key)
        with table.batch(batch_size=1000) as b:
            for value in values:
                b.delete(value)
        
if __name__ == '__main__':
    MRJob_delete_measures.run()  