include ../makefile.inc

all: librbf.a rbftest rbftest11a rbftest11b

# lib file dependencies
librbf.a: librbf.a(pfm.o)  # and possibly other .o files
librbf.a: librbf.a(rbfm.o)

# c file dependencies
pfm.o: pfm.h
rbfm.o: rbfm.h

rbftest.o: pfm.h rbfm.h

rbftest11a.o: pfm.h rbfm.h

rbftest11b.o: pfm.h rbfm.h

# binary dependencies
rbftest: rbftest.o librbf.a $(CODEROOT)/rbf/librbf.a

rbftest11a: rbftest11a.o librbf.a $(CODEROOT)/rbf/librbf.a

rbftest11b: rbftest11b.o librbf.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm rbftest rbftest11a rbftest11b *.a *.o *~

