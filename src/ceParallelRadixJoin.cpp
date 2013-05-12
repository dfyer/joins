/* used functions
int64_t ceParallelRadixJoin::join_init_run (vector<SAP_UINT> *relR, vector<SAP_UINT> *relC, JoinFunction jf, int nthreads)
int ceParallelRadixJoin::numa_localize(tuple_t * relation, int32_t num_tuples, uint32_t nthreads) 
void * ceParallelRadixJoin::numa_localize_thread(void * args) 

used libraries
#include <unistd.h> (numa_localize(..))
*/


static void *
alloc_aligned(size_t size)
{
    void * ret;
    int rv;
    rv = posix_memalign((void**)&ret, CACHE_LINE_SIZE, size); //CACHE_LINE_SIZE == 64

    if (rv) { 
        perror("alloc_aligned() failed: out of memory");
        return 0; 
    }
    
    return ret;
}

/* from task_queue.h start*/
struct task_t {
    vector<SAP_UINT> relR;
    vector<SAP_UINT> tmpR;
    vector<SAP_UINT> relS;
    vector<SAP_UINT> tmpS;
    task_t *   next;
};
typedef struct task_t task_t;

struct task_list_t {
    task_t *      tasks;
    task_list_t * next;
    int           curr;
};
typedef struct task_list_t task_list_t;

struct task_queue_t {
    pthread_mutex_t lock;
    pthread_mutex_t alloc_lock;
    task_t *        head;
    task_list_t *   free_list;
    int32_t         count;
    int32_t         alloc_size;
};
typedef struct task_queue_t task_queue_t;

inline 
task_t * 
get_next_task(task_queue_t * tq) __attribute__((always_inline));

inline 
void 
add_tasks(task_queue_t * tq, task_t * t) __attribute__((always_inline));

inline 
task_t * 
get_next_task(task_queue_t * tq) 
{
    pthread_mutex_lock(&tq->lock);
    task_t * ret = 0;
    if(tq->count > 0){
        ret = tq->head;
        tq->head = ret->next;
        tq->count --;
    }
    pthread_mutex_unlock(&tq->lock);

    return ret;
}

inline 
void 
add_tasks(task_queue_t * tq, task_t * t) 
{
    pthread_mutex_lock(&tq->lock);
    t->next = tq->head;
    tq->head = t;
    tq->count ++;
    pthread_mutex_unlock(&tq->lock);
}

/* atomically get the next available task */
inline 
task_t * 
task_queue_get_atomic(task_queue_t * tq) __attribute__((always_inline));

/* atomically add a task */
inline 
void 
task_queue_add_atomic(task_queue_t * tq, task_t * t) 
    __attribute__((always_inline));

inline 
void 
task_queue_add(task_queue_t * tq, task_t * t) __attribute__((always_inline));

inline 
void 
task_queue_copy_atomic(task_queue_t * tq, task_t * t)
    __attribute__((always_inline));

/* get a free slot of task_t */
inline 
task_t * 
task_queue_get_slot_atomic(task_queue_t * tq) __attribute__((always_inline));

inline 
task_t * 
task_queue_get_slot(task_queue_t * tq) __attribute__((always_inline));

/* initialize a task queue with given allocation block size */
task_queue_t * 
task_queue_init(int alloc_size);

void 
task_queue_free(task_queue_t * tq);

/**************** DEFINITIONS ********************************************/

inline 
task_t * 
task_queue_get_atomic(task_queue_t * tq) 
{
    pthread_mutex_lock(&tq->lock);
    task_t * ret = 0;
    if(tq->count > 0){
        ret      = tq->head;
        tq->head = ret->next;
        tq->count --;
    }
    pthread_mutex_unlock(&tq->lock);

    return ret;
}

inline 
void 
task_queue_add_atomic(task_queue_t * tq, task_t * t) 
{
    pthread_mutex_lock(&tq->lock);
    t->next  = tq->head;
    tq->head = t;
    tq->count ++;
    pthread_mutex_unlock(&tq->lock);

}

inline 
void 
task_queue_add(task_queue_t * tq, task_t * t) 
{
    t->next  = tq->head;
    tq->head = t;
    tq->count ++;
}
/* from task_queue.h end*/

/** holds the arguments passed to each thread */
struct arg_t {
    int32_t ** histR;
    vector<SAP_UINT> *  relR;
    vector<SAP_UINT> *  tmpR;
    int32_t ** histS;
    vector<SAP_UINT> *  relS;
    vector<SAP_UINT> *  tmpS;

    int32_t totalR;
    int32_t totalS;

    task_queue_t *      join_queue;
    task_queue_t *      part_queue;
	
    task_queue_t *      skew_queue;
    task_t **           skewtask;

    pthread_barrier_t * barrier;
    int64_t result;
    int32_t my_tid;
    int     nthreads;

    /* stats about the thread */
    int32_t        parts_processed;
    //uint64_t       timer1, timer2, timer3;
    //struct timeval start, end;
} __attribute__((aligned(CACHE_LINE_SIZE)));

int64_t ceParallelRadixJoin::join_init_run (vector<SAP_UINT> *relR, vector<SAP_UINT> *relC, int nthreads)
{
	//#define CPU_ZERO(PTR) (*(PTR) = 0)
	//#define CPU_SET(N, PTR) (*(PTR) = (N))
	//#define pthread_attr_setaffinity_np(ATTR, SZ, PTR) setaffinity(ATTR, SZ, PTR)
	//#define sched_setaffinity(A, SZ, PTR) setaffinity(A, SZ, PTR)
    int i, rv;
    pthread_t tid[nthreads];
    pthread_attr_t attr;
    pthread_barrier_t barrier;
    int set; //cpu_set_t set;
    arg_t args[nthreads];

    int32_t ** histR, ** histS;
    //tuple_t * tmpRelR, * tmpRelS;
    int32_t numperthr[2];
    int64_t result = 0;

    task_queue_t * part_queue, * join_queue;
    
	task_queue_t * skew_queue;
    task_t * skewtask = NULL;
    skew_queue = task_queue_init(FANOUT_PASS1);
    
	part_queue = task_queue_init(FANOUT_PASS1);
    join_queue = task_queue_init((1<<NUM_RADIX_BITS));


    /* allocate temporary space for partitioning */
    //tmpRelR = (tuple_t*) alloc_aligned(relR->size() * sizeof(SAP_UINT) +
    //                                   RELATION_PADDING);
    //tmpRelS = (tuple_t*) alloc_aligned(relS->size() * sizeof(SAP_UINT) +
    //                                   RELATION_PADDING);
    //MALLOC_CHECK((tmpRelR && tmpRelS));
	vector<SAP_UINT> tmpRelR, tmpRelS;

    /** Not an elegant way of passing whether we will numa-localize, but this
        feature is experimental anyway. */
    if(numalocalize) {
        numa_localize(&tmpRelR, relR->size(), nthreads);
        numa_localize(&tmpRelS, relS->size(), nthreads);
    }

    
    /* allocate histograms arrays, actual allocation is local to threads */
    histR = (SAP_UINT**) alloc_aligned(nthreads * sizeof(SAP_UINT*));
    histS = (SAP_UINT**) alloc_aligned(nthreads * sizeof(SAP_UINT*));
    MALLOC_CHECK((histR && histS));

    rv = pthread_barrier_init(&barrier, NULL, nthreads);
    if(rv != 0){
        printf("[ERROR] Couldn't create the barrier\n");
        exit(EXIT_FAILURE);
    }

    pthread_attr_init(&attr);

    /* first assign chunks of relR & relS for each thread */
    numperthr[0] = relR->size() / nthreads;
    numperthr[1] = relS->size() / nthreads;
    for(i = 0; i < nthreads; i++){
        int cpu_idx = get_cpu_id(i);

        //DEBUGMSG(1, "Assigning thread-%d to CPU-%d\n", i, cpu_idx);

        *(&set) = 0; //CPU_ZERO(&set);
        *(&set) = cpu_idx; //CPU_SET(cpu_idx, &set);
        pthread_attr_setaffinity_np(&attr, sizeof(int), &set);

        int32_t numR = (i == (nthreads-1)) ? 
            (relR->size() - i * numperthr[0]) : numperthr[0];
        int32_t numS = (i == (nthreads-1)) ? 
            (relS->size() - i * numperthr[1]) : numperthr[1];

		vector<SAP_UINT> cpRelR(relR.begin() + (i * numperthr[0]), relR.begin + numR);
		args[i].relR = cpRelR;
        //args[i].relR = relR->tuples + i * numperthr[0];
        args[i].tmpR = tmpRelR;
        args[i].histR = histR;

		vector<SAP_UINT> cpRelS(relS.begin() + (i * numperthr[0]), relS.begin + numS);
		args[i].relS = cpRelS;
        //args[i].relS = relS->tuples + i * numperthr[1];
        args[i].tmpS = tmpRelS;
        args[i].histS = histS;

        args[i].totalR = relR->size();
        args[i].totalS = relS->size();

        args[i].my_tid = i;
        args[i].part_queue = part_queue;
        args[i].join_queue = join_queue;

        args[i].skew_queue = skew_queue;
        args[i].skewtask   = &skewtask;

        args[i].barrier = &barrier;
        args[i].nthreads = nthreads;

        rv = pthread_create(&tid[i], &attr, prj_thread, (void*)&args[i]);
        //if (rv){
        //    printf("[ERROR] return code from pthread_create() is %d\n", rv);
        //    exit(-1);
        //}
    }

    /* wait for threads to finish */
    for(i = 0; i < nthreads; i++){
        pthread_join(tid[i], NULL);
        result += args[i].result;
    }

	/* #define ABSDIFF(X,Y) (((X) > (Y)) ? ((X)-(Y)) : ((Y)-(X))) */
    //fprintf(stdout, "TID JTASKS T1.1 T1.1-IDLE T1.2 T1.2-IDLE "\
    //        "T3 T3-IDLE T4 T4-IDLE T5 T5-IDLE\n");
    //for(i = 0; i < nthreads; i++){
    //    synctimer_t * glob = args[0].globaltimer;
    //    synctimer_t * local = & args[i].localtimer;
    //    fprintf(stdout,
    //            "%d %d %llu %llu %llu %llu %llu %llu %llu %llu "\
    //            "%llu %llu\n",
    //            (i+1), args[i].parts_processed, local->sync1[0], 
    //            glob->sync1[0] - local->sync1[0], 
    //            local->sync1[1] - glob->sync1[0],
    //            glob->sync1[1] - local->sync1[1],
    //            local->sync3 - glob->sync1[1],
    //            glob->sync3 - local->sync3,
    //            local->sync4 - glob->sync3,
    //            glob->sync4 - local->sync4,
    //            local->finish_time - glob->sync4,
    //            glob->finish_time - local->finish_time);
    //}

    /* clean up */
    for(i = 0; i < nthreads; i++) {
        free(histR[i]);
        free(histS[i]);
    }
    free(histR);
    free(histS);
    task_queue_free(part_queue);
    task_queue_free(join_queue);
    
	task_queue_free(skew_queue);
    
	free(tmpRelR);
    free(tmpRelS);

    return result;
}

/** 
 * The main thread of parallel radix join. It does partitioning in parallel with
 * other threads and during the join phase, picks up join tasks from the task
 * queue and calls appropriate JoinFunction to compute the join task.
 * 
 * @param param 
 * 
 * @return 
 */
void * 
prj_thread(void * param)
{
    arg_t * args   = (arg_t*) param;
    int32_t my_tid = args->my_tid;

    const int fanOut = 1 << (NUM_RADIX_BITS / NUM_PASSES);
    const int R = (NUM_RADIX_BITS / NUM_PASSES);
    const int D = (NUM_RADIX_BITS - (NUM_RADIX_BITS / NUM_PASSES));
    const int thresh1 = MAX((1<<D), (1<<R)) * THRESHOLD1(args->nthreads);

    uint64_t results = 0;
    int i;
    int rv;    

    part_t part;
    task_t * task;
    task_queue_t * part_queue;
    task_queue_t * join_queue;
#ifdef SKEW_HANDLING
    task_queue_t * skew_queue;
#endif

    int32_t * outputR = (int32_t *) calloc((fanOut+1), sizeof(int32_t));
    int32_t * outputS = (int32_t *) calloc((fanOut+1), sizeof(int32_t));
    MALLOC_CHECK((outputR && outputS));

    part_queue = args->part_queue;
    join_queue = args->join_queue;
#ifdef SKEW_HANDLING
    skew_queue = args->skew_queue;
#endif

    args->histR[my_tid] = (int32_t *) calloc(fanOut, sizeof(int32_t));
    args->histS[my_tid] = (int32_t *) calloc(fanOut, sizeof(int32_t));

    /* in the first pass, partitioning is done together by all threads */

    args->parts_processed = 0;

#ifdef PERF_COUNTERS
    if(my_tid == 0){
        PCM_initPerformanceMonitor(NULL, NULL);
        PCM_start();
    }
#endif

    /* wait at a barrier until each thread starts and then start the timer */
    BARRIER_ARRIVE(args->barrier, rv);

    /* if monitoring synchronization stats */
    SYNC_TIMERS_START(args, my_tid);

#ifndef NO_TIMING
    if(my_tid == 0){
        /* thread-0 checkpoints the time */
        gettimeofday(&args->start, NULL);
        startTimer(&args->timer1);
        startTimer(&args->timer2);
        startTimer(&args->timer3);
    }
#endif
    
    /********** 1st pass of multi-pass partitioning ************/
    part.R       = 0;
    part.D       = NUM_RADIX_BITS / NUM_PASSES;
    part.thrargs = args;
    part.padding = PADDING_TUPLES;

    /* 1. partitioning for relation R */
    part.rel          = args->relR;
    part.tmp          = args->tmpR;
    part.hist         = args->histR;
    part.output       = outputR;
    part.num_tuples   = args->numR;
    part.total_tuples = args->totalR;
    part.relidx       = 0;
    
#ifdef USE_SWWC_OPTIMIZED_PART
    parallel_radix_partition_optimized(&part);
#else
    parallel_radix_partition(&part);
#endif

    /* 2. partitioning for relation S */
    part.rel          = args->relS;
    part.tmp          = args->tmpS;
    part.hist         = args->histS;
    part.output       = outputS;
    part.num_tuples   = args->numS;
    part.total_tuples = args->totalS;
    part.relidx       = 1;
    
#ifdef USE_SWWC_OPTIMIZED_PART
    parallel_radix_partition_optimized(&part);
#else
    parallel_radix_partition(&part);
#endif


    /* wait at a barrier until each thread copies out */
    BARRIER_ARRIVE(args->barrier, rv);

    /********** end of 1st partitioning phase ******************/

    /* 3. first thread creates partitioning tasks for 2nd pass */
    if(my_tid == 0) {
        for(i = 0; i < fanOut; i++) {
            int32_t ntupR = outputR[i+1] - outputR[i] - PADDING_TUPLES;
            int32_t ntupS = outputS[i+1] - outputS[i] - PADDING_TUPLES;

#ifdef SKEW_HANDLING
            if(ntupR > thresh1 || ntupS > thresh1){
                DEBUGMSG(1, "Adding to skew_queue= R:%d, S:%d\n", ntupR, ntupS);

                task_t * t = task_queue_get_slot(skew_queue);

                t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
                t->relR.tuples = args->tmpR + outputR[i];
                t->tmpR.tuples = args->relR + outputR[i];

                t->relS.num_tuples = t->tmpS.num_tuples = ntupS;
                t->relS.tuples = args->tmpS + outputS[i];
                t->tmpS.tuples = args->relS + outputS[i];

                task_queue_add(skew_queue, t);
            } 
            else
#endif
            if(ntupR > 0 && ntupS > 0) {
                task_t * t = task_queue_get_slot(part_queue);

                t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
                t->relR.tuples = args->tmpR + outputR[i];
                t->tmpR.tuples = args->relR + outputR[i];

                t->relS.num_tuples = t->tmpS.num_tuples = ntupS;
                t->relS.tuples = args->tmpS + outputS[i];
                t->tmpS.tuples = args->relS + outputS[i];

                task_queue_add(part_queue, t);
            }
        }

        /* debug partitioning task queue */
        DEBUGMSG(1, "Pass-2: # partitioning tasks = %d\n", part_queue->count);

    }

    SYNC_TIMER_STOP(&args->localtimer.sync3);
    /* wait at a barrier until first thread adds all partitioning tasks */
    BARRIER_ARRIVE(args->barrier, rv);
    /* global barrier sync point-3 */
    SYNC_GLOBAL_STOP(&args->globaltimer->sync3, my_tid);

    /************ 2nd pass of multi-pass partitioning ********************/
    /* 4. now each thread further partitions and add to join task queue **/

#if NUM_PASSES==1
    /* If the partitioning is single pass we directly add tasks from pass-1 */
    task_queue_t * swap = join_queue;
    join_queue = part_queue;
    /* part_queue is used as a temporary queue for handling skewed parts */
    part_queue = swap;
    
#elif NUM_PASSES==2

    while((task = task_queue_get_atomic(part_queue))){

        serial_radix_partition(task, join_queue, R, D);

    }

#else
#warning Only 2-pass partitioning is implemented, set NUM_PASSES to 2!
#endif
    
#ifdef SKEW_HANDLING
    /* Partitioning pass-2 for skewed relations */
    part.R         = R;
    part.D         = D;
    part.thrargs   = args;
    part.padding   = SMALL_PADDING_TUPLES;

    while(1) {
        if(my_tid == 0) {
            *args->skewtask = task_queue_get_atomic(skew_queue);
        }
        BARRIER_ARRIVE(args->barrier, rv);
        if( *args->skewtask == NULL)
            break;

        DEBUGMSG((my_tid==0), "Got skew task = R: %d, S: %d\n", 
                 (*args->skewtask)->relR.num_tuples,
                 (*args->skewtask)->relS.num_tuples);

        int32_t numperthr = (*args->skewtask)->relR.num_tuples / args->nthreads;
        const int fanOut2 = (1 << D);

        free(outputR);
        free(outputS);

        outputR = (int32_t*) calloc(fanOut2 + 1, sizeof(int32_t));
        outputS = (int32_t*) calloc(fanOut2 + 1, sizeof(int32_t));

        free(args->histR[my_tid]);
        free(args->histS[my_tid]);

        args->histR[my_tid] = (int32_t*) calloc(fanOut2, sizeof(int32_t));
        args->histS[my_tid] = (int32_t*) calloc(fanOut2, sizeof(int32_t));

        /* wait until each thread allocates memory */
        BARRIER_ARRIVE(args->barrier, rv);

        /* 1. partitioning for relation R */
        part.rel          = (*args->skewtask)->relR.tuples + my_tid * numperthr;
        part.tmp          = (*args->skewtask)->tmpR.tuples;
        part.hist         = args->histR;
        part.output       = outputR;
        part.num_tuples   = (my_tid == (args->nthreads-1)) ? 
                            ((*args->skewtask)->relR.num_tuples - my_tid * numperthr) 
                            : numperthr;
        part.total_tuples = (*args->skewtask)->relR.num_tuples;
        part.relidx       = 2; /* meaning this is pass-2, no syncstats */
        parallel_radix_partition(&part);

        numperthr = (*args->skewtask)->relS.num_tuples / args->nthreads;
        /* 2. partitioning for relation S */
        part.rel          = (*args->skewtask)->relS.tuples + my_tid * numperthr;
        part.tmp          = (*args->skewtask)->tmpS.tuples;
        part.hist         = args->histS;
        part.output       = outputS;
        part.num_tuples   = (my_tid == (args->nthreads-1)) ? 
                            ((*args->skewtask)->relS.num_tuples - my_tid * numperthr)
                            : numperthr;
        part.total_tuples = (*args->skewtask)->relS.num_tuples;
        part.relidx       = 2; /* meaning this is pass-2, no syncstats */
        parallel_radix_partition(&part);

        /* wait at a barrier until each thread copies out */
        BARRIER_ARRIVE(args->barrier, rv);

        /* first thread adds join tasks */
        if(my_tid == 0) {
            const int THR1 = THRESHOLD1(args->nthreads);

            for(i = 0; i < fanOut2; i++) {
                int32_t ntupR = outputR[i+1] - outputR[i] - SMALL_PADDING_TUPLES;
                int32_t ntupS = outputS[i+1] - outputS[i] - SMALL_PADDING_TUPLES;
                if(ntupR > THR1 || ntupS > THR1){

                    DEBUGMSG(1, "Large join task = R: %d, S: %d\n", ntupR, ntupS);

                    /* use part_queue temporarily */
                    for(int k=0; k < args->nthreads; k++) {
                        int ns = (k == args->nthreads-1)
                                 ? (ntupS - k*(ntupS/args->nthreads))
                                 : (ntupS/args->nthreads);
                        task_t * t = task_queue_get_slot(part_queue);

                        t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
                        t->relR.tuples = (*args->skewtask)->tmpR.tuples + outputR[i];
                        t->tmpR.tuples = (*args->skewtask)->relR.tuples + outputR[i];

                        t->relS.num_tuples = t->tmpS.num_tuples = ns; //ntupS;
                        t->relS.tuples = (*args->skewtask)->tmpS.tuples + outputS[i] //;
                                         + k*(ntupS/args->nthreads);
                        t->tmpS.tuples = (*args->skewtask)->relS.tuples + outputS[i] //;
                                         + k*(ntupS/args->nthreads);

                        task_queue_add(part_queue, t);
                    }
                } 
                else
                if(ntupR > 0 && ntupS > 0) {
                    task_t * t = task_queue_get_slot(join_queue);

                    t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
                    t->relR.tuples = (*args->skewtask)->tmpR.tuples + outputR[i];
                    t->tmpR.tuples = (*args->skewtask)->relR.tuples + outputR[i];

                    t->relS.num_tuples = t->tmpS.num_tuples = ntupS;
                    t->relS.tuples = (*args->skewtask)->tmpS.tuples + outputS[i];
                    t->tmpS.tuples = (*args->skewtask)->relS.tuples + outputS[i];

                    task_queue_add(join_queue, t);

                    DEBUGMSG(1, "Join added = R: %d, S: %d\n", 
                           t->relR.num_tuples, t->relS.num_tuples);
                }
            }

        }
    }

    /* add large join tasks in part_queue to the front of the join queue */
    if(my_tid == 0) {
        while((task = task_queue_get_atomic(part_queue)))
            task_queue_add(join_queue, task);
    }

#endif

    free(outputR);
    free(outputS);

    SYNC_TIMER_STOP(&args->localtimer.sync4);
    /* wait at a barrier until all threads add all join tasks */
    BARRIER_ARRIVE(args->barrier, rv);
    /* global barrier sync point-4 */
    SYNC_GLOBAL_STOP(&args->globaltimer->sync4, my_tid);

#ifndef NO_TIMING
    if(my_tid == 0) stopTimer(&args->timer3);/* partitioning finished */
#endif

    DEBUGMSG((my_tid == 0), "Number of join tasks = %d\n", join_queue->count);

#ifdef PERF_COUNTERS
    if(my_tid == 0){
        PCM_stop();
        PCM_log("======= Partitioning phase profiling results ======\n");
        PCM_printResults();
        PCM_start();
    }
    /* Just to make sure we get consistent performance numbers */
    BARRIER_ARRIVE(args->barrier, rv);
#endif

    while((task = task_queue_get_atomic(join_queue))){
        /* do the actual join. join method differs for different algorithms,
           i.e. bucket chaining, histogram-based, histogram-based with simd &
           prefetching  */
        results += args->join_function(&task->relR, &task->relS, &task->tmpR);
                
        args->parts_processed ++;
    }

    args->result = results;
    /* this thread is finished */
    SYNC_TIMER_STOP(&args->localtimer.finish_time);

#ifndef NO_TIMING
    /* this is for just reliable timing of finish time */
    BARRIER_ARRIVE(args->barrier, rv);
    if(my_tid == 0) {
        /* Actually with this setup we're not timing build */
        stopTimer(&args->timer2);/* build finished */
        stopTimer(&args->timer1);/* probe finished */
        gettimeofday(&args->end, NULL);
    }
#endif

    /* global finish time */
    SYNC_GLOBAL_STOP(&args->globaltimer->finish_time, my_tid);

#ifdef PERF_COUNTERS
    if(my_tid == 0) {
        PCM_stop();
        PCM_log("=========== Build+Probe profiling results =========\n");
        PCM_printResults();
        PCM_log("===================================================\n");
        PCM_cleanup();
    }
    /* Just to make sure we get consistent performance numbers */
    BARRIER_ARRIVE(args->barrier, rv);
#endif

    return 0;
}

/* structs used in numa_localize(..) */
// 1
struct create_arg_t {
	vector<SAP_UINT> rel;
	uint32_t firstkey;
};
typedef struct create_arg_t create_arg_t;

// 2
int inited = 0;
int max_cpus;
int node_mapping[512];

int ceParallelRadixJoin::get_cpu_id(int thread_id) {
	if(!inited) {
		int ret = 0;

		max_cpus = 8;
		int mapping[8] = {0, 1, 2, 3, 8, 9, 10, 11};

		for(int i = 0; i < max_cpus; i++)
			node_mapping[i] = mapping[i % 8];
			ret = 1;
		}

		if (ret == 0) {
			int i;
			
			max_cpus = sysconf(_SC_NPROCESSORS_ONLN);
			for(i = 0; i < max_cpus; i++) {
				node_mapping[i] = i;
			}
		}

		inited = 1;
	}
	return node_mappiong[thread_id % max_cpus];
}

int ceParallelRadixJoin::numa_localize(vector<SAP_UINT> *relation, int32_t num_tuples, uint32_t nthreads) 
{
	//#define CPU_ZERO(PTR) (*(PTR) = 0)
	//#define CPU_SET(N, PTR) (*(PTR) = (N))
	//#define pthread_attr_setaffinity_np(ATTR, SZ, PTR) setaffinity(ATTR, SZ, PTR)
	//#define sched_setaffinity(A, SZ, PTR) setaffinity(A, SZ, PTR)
    uint32_t i, rv;
    uint32_t offset = 0;

    /* we need aligned allocation of items */
    create_arg_t args[nthreads];
    pthread_t tid[nthreads];
	int set; //cpu_set_t set;
    pthread_attr_t attr;

    unsigned int pagesize;
    unsigned int npages; unsigned int npages_perthr; unsigned int ntuples_perthr;
    unsigned int ntuples_lastthr;

    pagesize        = getpagesize(); // #include <unistd.h>
    npages          = (num_tuples * sizeof(SAP_UINT)) / pagesize + 1;
    npages_perthr   = npages / nthreads;
    ntuples_perthr  = npages_perthr * (pagesize/sizeof(SAP_UINT));
    ntuples_lastthr = num_tuples - ntuples_perthr * (nthreads-1);

    pthread_attr_init(&attr);

    for( i = 0; i < nthreads; i++ ) {
        int cpu_idx = get_cpu_id(i); //
        
		*(&set) = 0; //CPU_ZERO(&set);
        *(&set) = cpu_idx; //CPU_SET(cpu_idx, &set);
        //setaffinity(&attr, sizeof(int), &set); //pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);

        args[i].firstkey       = offset + 1;
		vector<int> tmpRel(relation->begin() + offset, lines->begin + offset + ((i == nthreads - 1) ? ntuples_lastthr : ntuples_perthr));
		args[i].rel = tmpRel;
        //args[i].rel.tupes      = relation + offset;
        //args[i].rel.num_tuples = (i == nthreads-1) ? ntuples_lastthr 
        //                         : ntuples_perthr;
        offset += ntuples_perthr;

        rv = pthread_create(&tid[i], &attr, ceParallelRadixJoin::numa_localize_thread, 
                            (void*)&args[i]);
        //if (rv){
        //    fprintf(stderr, "[ERROR] pthread_create() return code is %d\n", rv);
        //    exit(-1);
        //}
    }

    for(i = 0; i < nthreads; i++){
        pthread_join(tid[i], NULL);
    }

    return 0;
}

/** 
 * Just initialize mem. to 0 for making sure it will be allocated numa-local 
 */
void * ceParallelRadixJoin::numa_localize_thread(void * args) 
{
    create_arg_t * arg = (create_arg_t *) args;
    vector<SAP_UINT> *   rel = & arg->rel;
    uint32_t i;
    
    for (i = 0; i < rel->size(); i++) {
        rel[i] = 0;
    }

    return 0;
}
