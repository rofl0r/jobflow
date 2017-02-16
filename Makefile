LINKLIBS=""

FNAME=jobflow

MAINFILE=$(FNAME).c
OUTFILE=$(FNAME).out

CFLAGS_OWN=-Wall -Wextra -std=c99 -D_GNU_SOURCE
CFLAGS_DBG=-g -O0
CFLAGS_OPT=-Os -s
CFLAGS_OPT_AGGRESSIVE=-O3 -s -flto -fwhole-program

TESTSRC=$(sort $(wildcard tests/*.c))
TESTS=$(TESTSRC:.c=.out)

-include config.mak

CFLAGS_RCB_OPT_AGGRESSIVE=$(DB_FLAGS) ${CFLAGS_OWN} ${CFLAGS_OPT_AGGRESSIVE} ${CFLAGS}
CFLAGS_RCB_OPT=$(DB_FLAGS) ${CFLAGS_OWN} ${CFLAGS_OPT} ${CFLAGS}
CFLAGS_RCB_DBG=$(DB_FLAGS) ${CFLAGS_OWN} ${CFLAGS_DBG} ${CFLAGS}

all: debug

optimized:
	CFLAGS="${CFLAGS_RCB_OPT} -s" rcb --force $(RCBFLAGS) ${MAINFILE} $(LINKLIBS)
	strip --remove-section .comment ${OUTFILE}

optimized-aggressive:
	CFLAGS="${CFLAGS_RCB_OPT_AGGRESSIVE} -s" rcb --force $(RCBFLAGS) ${MAINFILE} $(LINKLIBS)
	strip --remove-section .comment ${OUTFILE}

odebug:
	CFLAGS="${CFLAGS_RCB_OPT} -g" rcb --force $(RCBFLAGS) ${MAINFILE} $(LINKLIBS)
	debug-stripper.sh $(OUTFILE)

debug:
	CFLAGS="${CFLAGS_RCB_DBG}" rcb --force $(RCBFLAGS) ${MAINFILE} $(LINKLIBS)

tests: $(TESTS)

tests/%.out: tests/%.c
	CFLAGS="${CFLAGS_RCB_DBG}" rcb --force $(RCBFLAGS) $<

.PHONY: all optimized optimized-aggressive debug odebug
