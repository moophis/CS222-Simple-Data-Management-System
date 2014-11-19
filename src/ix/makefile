
include ../makefile.inc

all: libix.a ixtest1 ixtest2 ixtest3 ixtest4a ixtest4b ixtest4c ixtest5 ixtest6 ixtest7 ixtest8 ixtest_extra_1 ixtest_extra_2 ixtest_extra_2a ixtest_extra_2b ixtest_extra_2c ixtest_extra_2d

# lib file dependencies
libix.a: libix.a(ix.o)  # and possibly other .o files

# c file dependencies
ix.o: ix.h

ixtest1.o: ixtest_util.h
ixtest2.o: ixtest_util.h
ixtest3.o: ixtest_util.h
ixtest4a.o: ixtest_util.h
ixtest4b.o: ixtest_util.h
ixtest4c.o: ixtest_util.h
ixtest5.o: ixtest_util.h
ixtest6.o: ixtest_util.h
ixtest7.o: ixtest_util.h
ixtest8.o: ixtest_util.h
ixtest_extra_1.o: ixtest_util.h
ixtest_extra_2.o: ixtest_util.h
ixtest_extra_2a.o: ixtest_util.h
ixtest_extra_2b.o: ixtest_util.h
ixtest_extra_2c.o: ixtest_util.h
ixtest_extra_2d.o: ixtest_util.h

# binary dependencies
ixtest1: ixtest1.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest2: ixtest2.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest3: ixtest3.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest4a: ixtest4a.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest4b: ixtest4b.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest4c: ixtest4c.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest5: ixtest5.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest6: ixtest6.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest7: ixtest7.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest8: ixtest8.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_extra_1: ixtest_extra_1.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_extra_2: ixtest_extra_2.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_extra_2a: ixtest_extra_2a.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_extra_2b: ixtest_extra_2b.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_extra_2c: ixtest_extra_2c.o libix.a $(CODEROOT)/rbf/librbf.a 
ixtest_extra_2d: ixtest_extra_2d.o libix.a $(CODEROOT)/rbf/librbf.a 

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm ixtest1 ixtest2 ixtest3 ixtest4a ixtest4b ixtest4c ixtest5 ixtest6 ixtest7 ixtest8 ixtest_extra_1 ixtest_extra_2 ixtest_extra_2a ixtest_extra_2b ixtest_extra_2c ixtest_extra_2d *.a *.o
	$(MAKE) -C $(CODEROOT)/rbf clean
