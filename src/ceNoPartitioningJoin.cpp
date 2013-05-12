int numalocalize;

struct bucket_t {
    volatile char     latch;
    /* 3B hole */
    uint32_t          count;
    SAP_UINT          tuples[BUCKET_SIZE];
    struct bucket_t * next;
};
typedef struct bucket_t        bucket_t;

struct ceNoPartitioningJoin::hashtable_t {
    bucket_t * buckets;
    int32_t    num_buckets;
    uint32_t   hash_mask;
    uint32_t   skip_bits;
};
typedef struct hashtable_t     hashtable_t;

int64_t ceNoPartitioningJoin::NPO_st(vector<SAP_UINT> *relR, vector<SAP_UINT> *relS, int nthreads)
{
    hashtable_t * ht;
    int64_t result = 0;
    uint32_t nbuckets = (relR->size() / BUCKET_SIZE);

    allocate_hashtable(&ht, nbuckets);

    build_hashtable_st(ht, relR);

    result = probe_hashtable(ht, relS);

	free(ht->buckets);
	free(ht);
    //destroy_hashtable(ht);

    return result;
}

/** 
 * Allocates a hashtable of NUM_BUCKETS and inits everything to 0. 
 * 
 * @param ht pointer to a hashtable_t pointer
 */
void 
ceNoPartitioningJoin::allocate_hashtable(hashtable_t ** ppht, uint32_t nbuckets)
{
    hashtable_t * ht;

    ht              = (hashtable_t*)malloc(sizeof(hashtable_t));
    ht->num_buckets = nbuckets;
    NEXT_POW_2((ht->num_buckets));

    /* allocate hashtable buckets cache line aligned */
    if (posix_memalign((void**)&ht->buckets, CACHE_LINE_SIZE,
                       ht->num_buckets * sizeof(bucket_t))){
        perror("Aligned allocation failed!\n");
        exit(EXIT_FAILURE);
    }

    /** Not an elegant way of passing whether we will numa-localize, but this
        feature is experimental anyway. */
	// hgpark: I tested with numalocalize == 0 option
    //if(numalocalize) {
    //    SAP_UINT * mem = (SAP_UINT *) ht->buckets;
    //    uint32_t ntuples = (ht->num_buckets*sizeof(bucket_t))/sizeof(SAP_UINT);
    //    numa_localize(mem, ntuples, nthreads);
    //}

    memset(ht->buckets, 0, ht->num_buckets * sizeof(bucket_t));
    ht->skip_bits = 0; /* the default for modulo hash */
    ht->hash_mask = (ht->num_buckets - 1) << ht->skip_bits;
    *ppht = ht;
}

/** 
 * Single-thread hashtable build method, ht is pre-allocated.
 * 
 * @param ht hastable to be built
 * @param rel the build relation
 */
void 
ceNoPartitioningJoin::build_hashtable_st(hashtable_t *ht, vector<SAP_UINT> *rel)
{
    uint32_t i;
    const uint32_t hashmask = ht->hash_mask;
    const uint32_t skipbits = ht->skip_bits;

    for(i=0; i < rel->size(); i++){
        tuple_t * dest;
        bucket_t * curr, * nxt;
        int32_t idx = HASH(rel[i], hashmask, skipbits);

        /* copy the tuple to appropriate hash bucket */
        /* if full, follow nxt pointer to find correct place */
        curr = ht->buckets + idx;
        nxt  = curr->next;

        if(curr->count == BUCKET_SIZE) {
            if(!nxt || nxt->count == BUCKET_SIZE) {
                bucket_t * b;
                b = (bucket_t*) calloc(1, sizeof(bucket_t));
                curr->next = b;
                b->next = nxt;
                b->count = 1;
                dest = b->tuples;
            }
            else {
                dest = nxt->tuples + nxt->count;
                nxt->count ++;
            }
        }
        else {
            dest = curr->tuples + curr->count;
            curr->count ++;
        }
        *dest = rel[i];
    }
}

/** 
 * Probes the hashtable for the given outer relation, returns num results. 
 * This probing method is used for both single and multi-threaded version.
 * 
 * @param ht hashtable to be probed
 * @param rel the probing outer relation
 * 
 * @return number of matching tuples
 */
int64_t 
ceNoPartitioningJoin::probe_hashtable(hashtable_t *ht, vector<SAP_UINT> *rel)
{
    uint32_t i, j;
    int64_t matches;

    const uint32_t hashmask = ht->hash_mask;
    const uint32_t skipbits = ht->skip_bits;
    
    matches = 0;

    for (i = 0; i < rel->size(); i++)
    {
        intkey_t idx = HASH(rel[i], hashmask, skipbits);
        bucket_t * b = ht->buckets+idx;

        do {
            for(j = 0; j < b->count; j++) {
                if(rel[i] == b->tuples[j].key){
                    matches ++;
                    /* TODO: we don't materialize the results. */
                }
            }

            b = b->next;/* follow overflow pointer */
        } while(b);
    }

    return matches;
}
