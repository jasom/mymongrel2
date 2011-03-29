;;;; mymongrel2.asd

(asdf:defsystem #:mymongrel2
  :serial t
  :depends-on (#:zeromq
               #:cl-json
	       #:tnetstring)
  :components ((:file "package")
               (:file "mymongrel2")))

