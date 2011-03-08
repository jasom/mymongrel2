;;;; package.lisp

(eval-when (:compile-toplevel :load-toplevel :execute)
  (cl:defpackage :mymongrel2
    (:use :common-lisp
	  :json)
    (:export :parse
	     :parse-netstring)))
