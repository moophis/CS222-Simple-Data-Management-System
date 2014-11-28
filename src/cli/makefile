include ../makefile.inc

all: libcli.a clitest_01 clitest_02 clitest_03 clitest_04 clitest_05 clitest_06 clitest_07 clitest_08 clitest_09 clitest_10 clitest_11 clitest_12 start

# lib file dependencies
libcli.a: libcli.a(cli.o)  # and possibly other .o files

# c file dependencies
cli.o: cli.h

clitest_01.o: cli.h

clitest_02.o: cli.h

clitest_03.o: cli.h

clitest_04.o: cli.h

clitest_05.o: cli.h

clitest_06.o: cli.h

clitest_07.o: cli.h

clitest_08.o: cli.h

clitest_09.o: cli.h

clitest_10.o: cli.h

clitest_11.o: cli.h

clitest_12.o: cli.h

start.o: cli.h

# binary dependencies
clitest_01: clitest_01.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_02: clitest_02.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_03: clitest_03.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_04: clitest_04.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_05: clitest_05.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_06: clitest_06.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_07: clitest_07.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_08: clitest_08.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_09: clitest_09.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_10: clitest_10.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_11: clitest_11.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

clitest_12: clitest_12.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

start: start.o libcli.a $(CODEROOT)/qe/libqe.a $(CODEROOT)/ix/libix.a $(CODEROOT)/rm/librm.a $(CODEROOT)/rbf/librbf.a

$(CODEROOT)/rm/librm.a:
	$(MAKE) -C $(CODEROOT)/rm librm.a

$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a $(CODEROOT)/rm/librm.a
$(CODEROOT)/ix/libix.a:
	$(MAKE) -C $(CODEROOT)/ix libix.a


.PHONY: $(CODEROOT)/pf/libpf.a $(CODEROOT)/rm/librm.a $(CODEROOT)/ix/libix.a
$(CODEROOT)/qe/libqe.a:
	$(MAKE) -C $(CODEROOT)/qe libqe.a

.PHONY: clean
clean:
	-rm clitest_01 clitest_02 clitest_03 clitest_04 clitest_05 clitest_06 clitest_07 clitest_08 clitest_09 clitest_10 clitest_11 clitest_12 start *.a *.o *~
	$(MAKE) -C $(CODEROOT)/rbf clean
	$(MAKE) -C $(CODEROOT)/rm clean
	$(MAKE) -C $(CODEROOT)/ix clean
	$(MAKE) -C $(CODEROOT)/qe clean
