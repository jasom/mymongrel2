;;;; package.lisp

(eval-when (:compile-toplevel :load-toplevel :execute)
  (cl:defpackage :mymongrel2
    (:use :common-lisp
	  :json)
    (:export 
	     :parse-netstring
             :format-chunk
             :format-http
             :with-connection
             :http-response
             :parse-netstring
             :request-disconnectp
             :connection
             :make-connection
             :close-connection
             :send
             :recv
             :recv-json
             :reply
             :reply-json
             :reply-http
             :reply-start-chunk
             :reply-a-cunk
             :reply-finish-chunk
             :deliver
             :deliver-json
             :deliver-http
             :reply-close
             :request-closep
             :deliver-close
             )))
