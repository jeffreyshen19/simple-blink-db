SimpleBlinkDB 
==================

A lightweight implementation of BlinkDB in Java.

## Getting the Data 

Because of size restrictions, some of the data files used for testing are not available in the repository. To run the existing test suite, download [test_dataset_50M.dat](https://drive.google.com/file/d/1MrO_Y6AQS7dpy6ZbPQXWdF5hPRRTeDQm/view?usp=sharing) and place it in the root directory. Additional test files can be generated using `generate_dataset.py`. 


## Running the Code 

### Generating Sample Families 

First, add the underlying base table (in this case `test_dataset_50M.dat`): 

```
// Create the TupleDesc 
Type types[] = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
String names[] = new String[]{"id", "quantity", "year"};
TupleDesc td = new TupleDesc(types, names);


// Load the file and add it to the catalog 
HeapFile hf = new HeapFile(new File("test_dataset_50M.dat"), td);
Database.getCatalog().addTable(hf, "t1");
```

Second, generate the uniform sample. To do this, specify how large the sample family should be. In this case, the sample family will contain 1,000,000 rows, and be segmented at 10,000 rows, 50,000 rows, and 100,000 rows. As a guiding metric, our samples are 2% of the original table's size. 
    
```
// Create sample table and add it to catalog
List<Integer> sampleSizes = Arrays.asList(10000, 50000, 100000, 1000000);
File f = new File("uniform-sample.dat"); // This is where the sample will be stored on disk 
SampleDBFile sf = new SampleDBFile(f, sampleSizes, null, this.td); // null refers to the fact this sample is not stratified
Database.getCatalog().addTable(sf, "sample-table-uniform", "", true); // true tells the catalog this DbFile is stratified

// Populate sample table (if it hasn't already been populated)
if(!f.exists()) {
   sf.createUniformSamples(this.hf);
   Database.getBufferPool().flushAllPages(); // Ensure it is written to memory 
}
```

Then, we want to generate the stratified samples. To do this, we can have our system tell us what columns to stratify on, given a storage cap: 
```
// Queries is an array of past queries, represented as OpIterators
List<OpIterator> queries = new ArrayList<>();
// SELECT COUNT/AVG/SUM(quantity) FROM table;
queries.add(new Aggregate(seqscan, 1, -1, Aggregator.Op.COUNT)); 
queries.add(new Aggregate(seqscan, 1, -1, Aggregator.Op.AVG)); 
queries.add(new Aggregate(seqscan, 1, -1, Aggregator.Op.SUM)); 
// Add more queries ....

int storageCap = 20000000; // 20MB
List<QueryColumnSet> stratifiedSamples = SampleCreator.getStratifiedSamplesToCreate(hf.getId(), queries, storageCap);
```

The list, `stratifiedSamples` gives us the list of columns we should generate stratified samples on. Let's generate these stratified samples!

```
for(int i = 0; i < stratifiedSamples.size(); i++){
   File stratifiedf = new File("sample-stratified.dat");
   QueryColumnSet qcs = stratifiedSamples.get(i);
   SampleDBFile stratifiedsf = new SampleDBFile(stratifiedf, sampleSizes, qcs, td);
   Database.getCatalog().addTable(stratifiedsf, "sample-table-stratified-" + i, "", true);
        
   // Populate sample table (if it hasn't already been populated)
   if(!stratifiedf.exists()) {
      stratifiedsf.createStratifiedSamples(hf);
      Database.getBufferPool().flushAllPages();
   }
}

Now we have all our samples and are ready to run! Sample generation can take several minutes to an hour, depending on how large the original file is. In the future, since the sample files are saved to disk, the lengthy process of sample generation will not have to be run again. 
