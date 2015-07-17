;;;; mymongrel2.asd

(asdf:defsystem #:mymongrel2
  :serial t
  :depends-on (#:pzmq
               #:cl-json
	       #:tnetstring)
  :components ((:file "package")
               (:file "mymongrel2")))

