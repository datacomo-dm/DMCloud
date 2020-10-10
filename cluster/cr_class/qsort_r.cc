#include <sys/types.h>
#include <stdlib.h>
#include <cr_class/cr_class.h>

static inline char	*med3(char *, char *, char *, int (*)(const void *, const void *, void *), void *);
static inline void	 swapfunc(char *, char *, size_t, int);

#define min(a, b)	(a) < (b) ? a : b

/*
 * Qsort routine from Bentley & McIlroy's "Engineering a Sort Function".
 */
#define SWAPCODE(TYPE, parmi, parmj, n) { 		\
	size_t i = (n) / sizeof (TYPE); 		\
	TYPE *pi = (TYPE *) (parmi); 			\
	TYPE *pj = (TYPE *) (parmj); 			\
	do { 						\
		TYPE	t = *pi;			\
		*pi++ = *pj;				\
		*pj++ = t;				\
        } while (--i > 0);				\
}

#define SWAPINIT(a, es) swaptype = ((char *)a - (char *)0) % sizeof(long) || \
	es % sizeof(long) ? 2 : es == sizeof(long)? 0 : 1;

static inline void
swapfunc(char *a, char *b, size_t n, int swaptype)
{
	if (swaptype <= 1)
		SWAPCODE(long, a, b, n)
	else
		SWAPCODE(char, a, b, n)
}

#define swap(a, b)					\
	if (swaptype == 0) {				\
		long t = *(long *)(a);			\
		*(long *)(a) = *(long *)(b);		\
		*(long *)(b) = t;			\
	} else						\
		swapfunc(a, b, es, swaptype)

#define vecswap(a, b, n) 	if ((n) > 0) swapfunc(a, b, n, swaptype)

static inline char *
med3(char *a, char *b, char *c, int (*cmp)(const void *, const void *, void *), void *cmp_arg)
{
	return cmp(a, b, cmp_arg) < 0 ?
	       (cmp(b, c, cmp_arg) < 0 ? b : (cmp(a, c, cmp_arg) < 0 ? c : a ))
              :(cmp(b, c, cmp_arg) > 0 ? b : (cmp(a, c, cmp_arg) < 0 ? a : c ));
}

int
CR_Class_NS::qsort_r(void *aa, size_t n, size_t es, sort_cmp_func_t cmp, void *cmp_arg)
{
	char *pa, *pb, *pc, *pd, *pl, *pm, *pn;
	int cmp_result, swaptype, swap_cnt;
	size_t d, r;
	char *a = (char *)aa;

	if (n <= 1)
		return (0);

	if (!es) {
		errno = EINVAL;
		return (-1);
	}

loop:	SWAPINIT(a, es);
	swap_cnt = 0;
	if (n < 7) {
		for (pm = (char *)a + es; pm < (char *) a + n * es; pm += es)
			for (pl = pm; pl > (char *) a && cmp(pl - es, pl, cmp_arg) > 0;
			     pl -= es)
				swap(pl, pl - es);
		return (0);
	}
	pm = (char *)a + (n / 2) * es;
	if (n > 7) {
		pl = (char *)a;
		pn = (char *)a + (n - 1) * es;
		if (n > 40) {
			d = (n / 8) * es;
			pl = med3(pl, pl + d, pl + 2 * d, cmp, cmp_arg);
			pm = med3(pm - d, pm, pm + d, cmp, cmp_arg);
			pn = med3(pn - 2 * d, pn - d, pn, cmp, cmp_arg);
		}
		pm = med3(pl, pm, pn, cmp, cmp_arg);
	}
	swap(a, pm);
	pa = pb = (char *)a + es;

	pc = pd = (char *)a + (n - 1) * es;
	for (;;) {
		while (pb <= pc && (cmp_result = cmp(pb, a, cmp_arg)) <= 0) {
			if (cmp_result == 0) {
				swap_cnt = 1;
				swap(pa, pb);
				pa += es;
			}
			pb += es;
		}
		while (pb <= pc && (cmp_result = cmp(pc, a, cmp_arg)) >= 0) {
			if (cmp_result == 0) {
				swap_cnt = 1;
				swap(pc, pd);
				pd -= es;
			}
			pc -= es;
		}
		if (pb > pc)
			break;
		swap(pb, pc);
		swap_cnt = 1;
		pb += es;
		pc -= es;
	}
	if (swap_cnt == 0) {  /* Switch to insertion sort */
		for (pm = (char *) a + es; pm < (char *) a + n * es; pm += es)
			for (pl = pm; pl > (char *) a && cmp(pl - es, pl, cmp_arg) > 0; 
			     pl -= es)
				swap(pl, pl - es);
		return (0);
	}

	pn = (char *)a + n * es;
	r = min(pa - (char *)a, pb - pa);
	vecswap(a, pb - r, r);
	r = min((uintptr_t)pd - (uintptr_t)pc, (uintptr_t)pn - (uintptr_t)pd - (uintptr_t)es);
	vecswap(pb, pn - r, r);
	if ((r = pb - pa) > es)
		CR_Class_NS::qsort_r(a, r / es, es, cmp, cmp_arg);
	if ((r = pd - pc) > es) {
		/* Iterate rather than recurse to save stack space */
		a = pn - r;
		n = r / es;
		goto loop;
	}
	return (0);
}
