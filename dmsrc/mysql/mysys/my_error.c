/* Copyright (C) 2000 MySQL AB

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include "mysys_priv.h"
#include "mysys_err.h"
#include <m_string.h>
#include <stdarg.h>
#include <m_ctype.h>

/* Define some external variables for error handling */

/*
  WARNING!
  my_error family functions have to be used according following rules:
  - if message have not parameters use my_message(ER_CODE, ER(ER_CODE), MYF(N))
  - if message registered use my_error(ER_CODE, MYF(N), ...).
  - With some special text of errror message use:
  my_printf_error(ER_CODE, format, MYF(N), ...)
*/

char NEAR errbuff[NRERRBUFFS][ERRMSGSIZE];

/*
  Message texts are registered into a linked list of 'my_err_head' structs.
  Each struct contains (1.) an array of pointers to C character strings with
  '\0' termination, (2.) the error number for the first message in the array
  (array index 0) and (3.) the error number for the last message in the array
  (array index (last - first)).
  The array may contain gaps with NULL pointers and pointers to empty strings.
  Both kinds of gaps will be translated to "Unknown error %d.", if my_error()
  is called with a respective error number.
  The list of header structs is sorted in increasing order of error numbers.
  Negative error numbers are allowed. Overlap of error numbers is not allowed.
  Not registered error numbers will be translated to "Unknown error %d.".
*/
static struct my_err_head
{
  struct my_err_head    *meh_next;      /* chain link */
  const char            **meh_errmsgs;  /* error messages array */
  int                   meh_first;      /* error number matching array slot 0 */
  int                   meh_last;       /* error number matching last slot */
} my_errmsgs_globerrs = {NULL, globerrs, EE_ERROR_FIRST, EE_ERROR_LAST};

static struct my_err_head *my_errmsgs_list= &my_errmsgs_globerrs;


/*
   Error message to user

   SYNOPSIS
     my_error()
       nr	Errno
       MyFlags	Flags
       ...	variable list

  RETURN
    What (*error_handler_hook)() returns:
    0   OK
*/

int my_error(int nr, myf MyFlags, ...)
{
  const char *format;
  struct my_err_head *meh_p;
  va_list args;
  char ebuff[ERRMSGSIZE + 20];
  DBUG_ENTER("my_error");
  DBUG_PRINT("my", ("nr: %d  MyFlags: %d  errno: %d", nr, MyFlags, errno));

  /* Search for the error messages array, which could contain the message. */
  for (meh_p= my_errmsgs_list; meh_p; meh_p= meh_p->meh_next)
    if (nr <= meh_p->meh_last)
      break;

  /* get the error message string. Default, if NULL or empty string (""). */
  if (! (format= (meh_p && (nr >= meh_p->meh_first)) ?
         meh_p->meh_errmsgs[nr - meh_p->meh_first] : NULL) || ! *format)
    (void) my_snprintf (ebuff, sizeof(ebuff), "Unknown error %d", nr);
  else
  {
    va_start(args,MyFlags);
    (void) my_vsnprintf (ebuff, sizeof(ebuff), format, args);
    va_end(args);
  }
  DBUG_RETURN((*error_handler_hook)(nr, ebuff, MyFlags));
}


/*
  Error as printf

  SYNOPSIS
    my_printf_error()
      error	Errno
      format	Format string
      MyFlags	Flags
      ...	variable list
*/

int my_printf_error(uint error, const char *format, myf MyFlags, ...)
{
  va_list args;
  char ebuff[ERRMSGSIZE+20];
  DBUG_ENTER("my_printf_error");
  DBUG_PRINT("my", ("nr: %d  MyFlags: %d  errno: %d  Format: %s",
		    error, MyFlags, errno, format));

  va_start(args,MyFlags);
  (void) my_vsnprintf (ebuff, sizeof(ebuff), format, args);
  va_end(args);
  DBUG_RETURN((*error_handler_hook)(error, ebuff, MyFlags));
}

/*
  Give message using error_handler_hook

  SYNOPSIS
    my_message()
      error	Errno
      str	Error message
      MyFlags	Flags
*/

int my_message(uint error, const char *str, register myf MyFlags)
{
  return (*error_handler_hook)(error, str, MyFlags);
}


/*
  Register error messages for use with my_error().

  SYNOPSIS
    my_error_register()
    errmsgs                     array of pointers to error messages
    first                       error number of first message in the array
    last                        error number of last message in the array

  DESCRIPTION
    The pointer array is expected to contain addresses to NUL-terminated
    C character strings. The array contains (last - first + 1) pointers.
    NULL pointers and empty strings ("") are allowed. These will be mapped to
    "Unknown error" when my_error() is called with a matching error number.
    This function registers the error numbers 'first' to 'last'.
    No overlapping with previously registered error numbers is allowed.

  RETURN
    0           OK
    != 0        Error
*/

int my_error_register(const char **errmsgs, int first, int last)
{
  struct my_err_head *meh_p;
  struct my_err_head **search_meh_pp;

  /* Allocate a new header structure. */
  if (! (meh_p= (struct my_err_head*) my_malloc(sizeof(struct my_err_head),
                                                MYF(MY_WME))))
    return 1;
  meh_p->meh_errmsgs= errmsgs;
  meh_p->meh_first= first;
  meh_p->meh_last= last;

  /* Search for the right position in the list. */
  for (search_meh_pp= &my_errmsgs_list;
       *search_meh_pp;
       search_meh_pp= &(*search_meh_pp)->meh_next)
  {
    if ((*search_meh_pp)->meh_last > first)
      break;
  }

  /* Error numbers must be unique. No overlapping is allowed. */
  if (*search_meh_pp && ((*search_meh_pp)->meh_first <= last))
  {
    my_free((uchar*)meh_p, MYF(0));
    return 1;
  }

  /* Insert header into the chain. */
  meh_p->meh_next= *search_meh_pp;
  *search_meh_pp= meh_p;
  return 0;
}


/*
  Unregister formerly registered error messages.

  SYNOPSIS
    my_error_unregister()
    first                       error number of first message
    last                        error number of last message

  DESCRIPTION
    This function unregisters the error numbers 'first' to 'last'.
    These must have been previously registered by my_error_register().
    'first' and 'last' must exactly match the registration.
    If a matching registration is present, the header is removed from the
    list and the pointer to the error messages pointers array is returned.
    Otherwise, NULL is returned.

  RETURN
    non-NULL    OK, returns address of error messages pointers array.
    NULL        Error, no such number range registered.
*/

const char **my_error_unregister(int first, int last)
{
  struct my_err_head    *meh_p;
  struct my_err_head    **search_meh_pp;
  const char            **errmsgs;

  /* Search for the registration in the list. */
  for (search_meh_pp= &my_errmsgs_list;
       *search_meh_pp;
       search_meh_pp= &(*search_meh_pp)->meh_next)
  {
    if (((*search_meh_pp)->meh_first == first) &&
        ((*search_meh_pp)->meh_last == last))
      break;
  }
  if (! *search_meh_pp)
    return NULL;

  /* Remove header from the chain. */
  meh_p= *search_meh_pp;
  *search_meh_pp= meh_p->meh_next;

  /* Save the return value and free the header. */
  errmsgs= meh_p->meh_errmsgs;
  my_free((uchar*) meh_p, MYF(0));
  
  return errmsgs;
}


void my_error_unregister_all(void)
{
  struct my_err_head    *list, *next;
  for (list= my_errmsgs_globerrs.meh_next; list; list= next)
  {
    next= list->meh_next;
    my_free((uchar*) list, MYF(0));
  }
  /*DT Project Modfied BEGIN */
  /* 客户端多次重复连接，引起释放异常的段错误，恢复链表头后正常 
    增加一行*/
  my_errmsgs_globerrs.meh_next=NULL;
  /*DT Project Modfied END */
  my_errmsgs_list= &my_errmsgs_globerrs;
}
