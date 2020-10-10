#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <cr_class/cr_class.h>

static inline char	*i_med3(char *, char *, char *, int (*)(const void *, const void *, void *), void *);
static inline void	 i_swapfunc(char *, char *, size_t, int);
static inline int	introsort_r_inner(void *, size_t, size_t,
				CR_Class_NS::sort_cmp_func_t, void *, size_t);

#define I_MIN(a, b)	(a) < (b) ? a : b

/*
 * Qsort routine from Bentley & McIlroy's "Engineering a Sort Function".
 */
#define I_SWAPCODE(TYPE, parmi, parmj, n) { 		\
	size_t i = (n) / sizeof (TYPE); 		\
	TYPE *pi = (TYPE *) (parmi); 			\
	TYPE *pj = (TYPE *) (parmj); 			\
	do { 						\
		TYPE	t = *pi;			\
		*pi++ = *pj;				\
		*pj++ = t;				\
        } while (--i > 0);				\
}

#define I_SWAPINIT(a, es) swaptype = ((char *)a - (char *)0) % sizeof(long) || \
	es % sizeof(long) ? 2 : es == sizeof(long)? 0 : 1;

static inline void
i_swapfunc(char *a, char *b, size_t n, int swaptype)
{
	if (swaptype <= 1)
		I_SWAPCODE(long, a, b, n)
	else
		I_SWAPCODE(char, a, b, n)
}

#define I_SWAP(a, b)					\
	if (swaptype == 0) {				\
		long t = *(long *)(a);			\
		*(long *)(a) = *(long *)(b);		\
		*(long *)(b) = t;			\
	} else						\
		i_swapfunc(a, b, es, swaptype)

#define I_VECSWAP(a, b, n) 	if ((n) > 0) i_swapfunc(a, b, n, swaptype)

static inline char *
i_med3(char *a, char *b, char *c, int (*cmp)(const void *, const void *, void *), void *cmp_arg)
{
	return cmp(a, b, cmp_arg) < 0 ?
	       (cmp(b, c, cmp_arg) < 0 ? b : (cmp(a, c, cmp_arg) < 0 ? c : a ))
              :(cmp(b, c, cmp_arg) > 0 ? b : (cmp(a, c, cmp_arg) < 0 ? a : c ));
}

static inline int
introsort_r_inner(void *aa, size_t n, size_t es,
    CR_Class_NS::sort_cmp_func_t cmp, void *cmp_arg, size_t intro_depth)
{
	if (intro_depth == 0) {
		return CR_Class_NS::heapsort_r(aa, n, es, cmp, cmp_arg);
	}

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

loop:	I_SWAPINIT(a, es);
	swap_cnt = 0;
	if (n < 7) {
		for (pm = (char *)a + es; pm < (char *) a + n * es; pm += es)
			for (pl = pm; pl > (char *) a && cmp(pl - es, pl, cmp_arg) > 0;
			     pl -= es)
				I_SWAP(pl, pl - es);
		return (0);
	}
	pm = (char *)a + (n / 2) * es;
	if (n > 7) {
		pl = (char *)a;
		pn = (char *)a + (n - 1) * es;
		if (n > 40) {
			d = (n / 8) * es;
			pl = i_med3(pl, pl + d, pl + 2 * d, cmp, cmp_arg);
			pm = i_med3(pm - d, pm, pm + d, cmp, cmp_arg);
			pn = i_med3(pn - 2 * d, pn - d, pn, cmp, cmp_arg);
		}
		pm = i_med3(pl, pm, pn, cmp, cmp_arg);
	}
	I_SWAP(a, pm);
	pa = pb = (char *)a + es;

	pc = pd = (char *)a + (n - 1) * es;
	for (;;) {
		while (pb <= pc && (cmp_result = cmp(pb, a, cmp_arg)) <= 0) {
			if (cmp_result == 0) {
				swap_cnt = 1;
				I_SWAP(pa, pb);
				pa += es;
			}
			pb += es;
		}
		while (pb <= pc && (cmp_result = cmp(pc, a, cmp_arg)) >= 0) {
			if (cmp_result == 0) {
				swap_cnt = 1;
				I_SWAP(pc, pd);
				pd -= es;
			}
			pc -= es;
		}
		if (pb > pc)
			break;
		I_SWAP(pb, pc);
		swap_cnt = 1;
		pb += es;
		pc -= es;
	}
	if (swap_cnt == 0) {  /* Switch to insertion sort */
		for (pm = (char *) a + es; pm < (char *) a + n * es; pm += es)
			for (pl = pm; pl > (char *) a && cmp(pl - es, pl, cmp_arg) > 0; 
			     pl -= es)
				I_SWAP(pl, pl - es);
		return (0);
	}

	pn = (char *)a + n * es;
	r = I_MIN(pa - (char *)a, pb - pa);
	I_VECSWAP(a, pb - r, r);
	r = I_MIN((uintptr_t)pd - (uintptr_t)pc, (uintptr_t)pn - (uintptr_t)pd - (uintptr_t)es);
	I_VECSWAP(pb, pn - r, r);
	if ((r = pb - pa) > es) {
		if (introsort_r_inner(a, r / es, es, cmp, cmp_arg, intro_depth - 1) < 0)
			return (-1);
	}
	if ((r = pd - pc) > es) {
		/* Iterate rather than recurse to save stack space */
		a = pn - r;
		n = r / es;
		goto loop;
	}
	return (0);
}

int
CR_Class_NS::introsort_r(void *base, size_t nmemb, size_t size, sort_cmp_func_t compar, void *compar_arg)
{
	return introsort_r_inner(base, nmemb, size, compar, compar_arg, CR_LOG_2_CEIL(nmemb));
}
