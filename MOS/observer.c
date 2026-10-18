/* Observation adapter for the vendored course S2 CPU.
 * No scheduler/semaphore behavior is implemented here. All values come from
 * the CPU registers and memory executed by course/sim21/s21.c.
 */
#include "s21.h"
#include "symbols.h"

extern int R[32], Ir, Pc, M[], RetAds;
extern int runflag, savePc, cpuflag;
extern int intflag, intmask[], intrq[];
/* Required by the linked course runtime's unused interactive entry point. */
int display = 0;
void interp(void) {}
void execute(void) {}

static FILE *tracefile;
static int initialized, sequence, pcbs[MOS_PROCESSES], stopped[MOS_PROCESSES], terminating[MOS_PROCESSES];
static int last_current, last_produced, last_consumed, sems[3], previous_sem[3];
static int previous_queue[3][MOS_PROCESSES], previous_count[3];
static const char *semnames[] = {"empty", "full", "mutex"};

static int memory(int a) {
    if (a < 0 || a >= MAXMEM) { fprintf(stderr, "invalid memory address %d\n", a); exit(2); }
    return M[a];
}

static int queue(int sem, int *out) {
    int header = memory(sem + 1), cell = memory(header), n = 0;
    while (cell) {
        if (n >= MOS_PROCESSES) { fprintf(stderr, "invalid semaphore queue\n"); exit(2); }
        out[n++] = memory(cell);
        cell = memory(cell + 1);
    }
    return n;
}

static int contains(int *list, int n, int value) {
    int i;
    for (i = 0; i < n; ++i) if (list[i] == value) return 1;
    return 0;
}

static void list(int *values, int n) {
    int i;
    fputc('[', tracefile);
    for (i = 0; i < n; ++i) fprintf(tracefile, "%s%d", i ? "," : "", values[i]);
    fputc(']', tracefile);
}

static void emit(const char *event, int subject, int sem, int value) {
    int i, j, q[MOS_PROCESSES], n, current = memory(G_currentp);
    int ready[MOS_PROCESSES], nr = 0;
    if (!initialized) return;
    fprintf(tracefile,
        "{\"seq\":%d,\"clock\":%d,\"event\":\"%s\",\"subject\":%d,\"semaphore\":%d,\"value\":%d,"
        "\"current\":%d,\"pc\":%d,\"instruction_pc\":%d,\"sp\":%d,\"fp\":%d,\"rads\":%d,\"retval\":%d,\"retads\":%d,"
        "\"intflag\":%d,\"timer_mask\":%d,\"timer_pending\":%d,\"nump\":%d,"
        "\"head\":%d,\"tail\":%d,\"occupancy\":%d,\"produced\":%d,\"consumed\":%d,",
        ++sequence, clock, event, subject, sem, value, current, Pc, savePc,
        R[29], R[30], R[27], R[28], RetAds, intflag, intmask[0], intrq[0], memory(G_nump),
        memory(G_head), memory(G_tail), memory(G_occupancy), memory(G_produced), memory(G_consumed));
    fprintf(tracefile, "\"semaphores\":{");
    for (i = 0; i < 3; ++i) {
        n = queue(sems[i], q);
        fprintf(tracefile, "%s\"%s\":{\"address\":%d,\"value\":%d,\"waiting\":", i ? "," : "", semnames[i], sems[i], memory(sems[i]));
        list(q, n);
        fputc('}', tracefile);
    }
    fprintf(tracefile, "},\"pcbs\":[");
    for (i = 0; i < MOS_PROCESSES; ++i) {
        int p = pcbs[i];
        if (memory(p + 5) && p != current) ready[nr++] = p;
        fprintf(tracefile, "%s{\"address\":%d,\"role\":\"%s\",\"terminated\":%d,\"context\":[", i ? "," : "", p, i == MOS_PROCESSES - 1 ? "producer" : "consumer", stopped[i]);
        for (j = 0; j < 6; ++j) fprintf(tracefile, "%s%d", j ? "," : "", memory(p + j));
        fprintf(tracefile, "],\"saved_registers\":[");
        /* pushm stores r0..r15 immediately below the saved SP. */
        for (j = 0; j < 16; ++j) fprintf(tracefile, "%s%d", j ? "," : "", memory(memory(p + 1) - 15 + j));
        fprintf(tracefile, "]}");
    }
    fprintf(tracefile, "],\"registers\":"); list(R, 32);
    fprintf(tracefile, ",\"buffer\":"); list(M + memory(G_buffer), MOS_CAPACITY);
    fprintf(tracefile, ",\"ready\":"); list(ready, nr);
    fprintf(tracefile, "}\n");
}

/* Called from the real simulator interrupt() before it updates RetAds/Pc. */
void mos_interrupt(int n) {
    int software = (IRop() == XOP && IRxop() == INT);
    emit(software ? "software_interrupt" : "timer_interrupt", memory(G_currentp), n, Pc);
}

static void begin(void) {
    int cell = memory(memory(G_Q)), i;
    for (i = 0; i < MOS_PROCESSES; ++i) {
        pcbs[i] = memory(cell);
        cell = memory(cell + 1);
    }
    sems[0] = memory(G_empty); sems[1] = memory(G_full); sems[2] = memory(G_mutex);
    for (i = 0; i < 3; ++i) {
        previous_sem[i] = memory(sems[i]);
        previous_count[i] = queue(sems[i], previous_queue[i]);
    }
    last_current = memory(G_currentp);
    initialized = 1;
    emit("init", 0, 0, MOS_CAPACITY);
}

static void before(void) {
    int i, p;
    if (!initialized && Pc == F_boot) begin();
    if (!initialized) return;
    if (Pc == F_wait) emit("wait", memory(G_currentp), memory(R[29]), 0);
    if (Pc == F_signal) emit("signal", memory(G_currentp), memory(R[29]), 0);
    if (Pc == F_blockp) emit("block", memory(G_currentp), 0, 0);
    if (Pc == F_wakeup) emit("wakeup", memory(R[29]), 0, 0);
    if (Pc == F_terminate) {
        p = memory(G_currentp);
        for (i = 0; i < MOS_PROCESSES; ++i) if (pcbs[i] == p) terminating[i] = 1;
        emit("terminate", p, 0, 0);
    }
}

void mos_after_instruction(void) {
    int i, j, q[MOS_PROCESSES], n, v;
    if (!initialized) return;
    for (i = 0; i < MOS_PROCESSES; ++i) {
        if (terminating[i] && !stopped[i] && memory(pcbs[i] + 5) == 0) {
            stopped[i] = 1;
            emit("terminated", pcbs[i], 0, 0);
        }
    }
    for (i = 0; i < 3; ++i) {
        n = queue(sems[i], q);
        for (j = 0; j < n; ++j)
            if (!contains(previous_queue[i], previous_count[i], q[j])) emit("queue_enter", q[j], sems[i], memory(sems[i]));
        for (j = 0; j < previous_count[i]; ++j)
            if (!contains(q, n, previous_queue[i][j])) emit("queue_leave", previous_queue[i][j], sems[i], memory(sems[i]));
        memcpy(previous_queue[i], q, sizeof(int) * n); previous_count[i] = n;
        v = memory(sems[i]);
        if (v != previous_sem[i]) { previous_sem[i] = v; emit("semaphore_value", memory(G_currentp), sems[i], v); }
    }
    v = memory(G_currentp);
    if (v != last_current) { last_current = v; emit("selected", v, 0, 0); }
    v = memory(G_produced);
    if (v != last_produced) { last_produced = v; emit("put", memory(G_currentp), 0, memory(G_lastWritten)); }
    v = memory(G_consumed);
    if (v != last_consumed) { last_consumed = v; emit("get", memory(G_currentp), 0, memory(G_lastTaken)); }
    if (IRop() == XOP && IRxop() == RETI) emit("dispatch", memory(G_currentp), 0, Pc);
    if (!runflag) emit("stop", memory(G_currentp), 0, 0);
}

int main(int argc, char **argv) {
    if (argc != 3) { fprintf(stderr, "usage: sim-trace object trace.jsonl\n"); return 2; }
    tracefile = fopen(argv[2], "w");
    if (!tracefile) return 2;
    init_io(); clock = 0; ninst = 0; Pc = 0; runflag = 1; cpuflag = 1;
    intflag = 1; intmask[0] = 1; intmask[1] = 0; intmask[2] = 0; intmask[3] = 0;
    loadprogram(argv[1]);
    while (runflag && clock < MOS_MAX_CLOCKS) {
        before(); runoneclock();
    }
    fclose(tracefile);
    if (runflag) { fprintf(stderr, "simulator exceeded %d clocks (possible deadlock)\n", MOS_MAX_CLOCKS); return 3; }
    printf("executed %d S2 instructions in %d simulation clocks\n", ninst, clock);
    return 0;
}
