
include ../makefile.inc

all: librbf.a rbftest rbftest11a rbftest11b rbftest12 rbftest13 rbftest14 rbftest15 rbftest16

# lib file dependencies
librbf.a: librbf.a(pfm.o)  # and possibly other .o files
librbf.a: librbf.a(rbfm.o)

# c file dependencies
pfm.o: pfm.h
rbfm.o: rbfm.h

rbftest.o: pfm.h rbfm.h
rbftest11a.o: pfm.h rbfm.h
rbftest11b.o: pfm.h rbfm.h
rbftest12.o: pfm.h rbfm.h
rbftest13.o: pfm.h rbfm.h
rbftest14.o: pfm.h rbfm.h
rbftest15.o: pfm.h rbfm.h
rbftest16.o: pfm.h rbfm.h

# binary dependencies
rbftest: rbftest.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest11a: rbftest11a.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest11b: rbftest11b.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest12: rbftest12.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest13: rbftest13.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest14: rbftest14.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest15: rbftest15.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest16: rbftest16.o librbf.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm rbftest rbftest11a rbftest11b rbftest12 rbftest13 rbftest14 rbftest15 rbftest16 *.a *.o *~
