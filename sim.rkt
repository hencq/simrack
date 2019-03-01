#lang racket

(require racket/contract
         racket/generator
         racket/generic
         data/heap
         data/queue
         math/distributions
         threading)

(provide
 (contract-out [make-sim (-> sim?)]
               [make-event (->* [sim?] [(listof (-> any))] event?)]
               [add-callback! (-> event? (-> any) any)]
               [trigger! (->* [event?] [number?] any)]
               [step! (-> sim? any)]
               [run! (-> sim? #:until number? any)]
               [wait (-> sim? number? event?)]
               [process (-> sim? (-> sim? (-> any)) event?)]
               [make-resource (-> sim? number? resource?)]
               [request (-> resource? event:request?)]
               [release (-> event:request? any)]))

(struct sim ([time #:mutable] queue idgen) #:transparent)

(define (make-sim)
  (define (event<=? x y)
    (or
     (<= (event-scheduled x) (event-scheduled y))
     (and (= (event-scheduled x) (event-scheduled y))
          (<= (event-id x) (event-id y)))))
  (define idgen (generator ()
                           (let loop ([id 0])
                             (yield id)
                             (loop (add1 id)))))
  (sim 0 (make-heap event<=?) idgen))

(define-generics cancelable
  (cancel cancelable))

(struct event (sim [callbacks #:mutable] [completed? #:mutable] [scheduled #:mutable] [id #:mutable] [value #:mutable]) #:transparent)

(define (make-event sim [callbacks '()] #:value [val #f])
  (event sim callbacks #f #f #f val))

(define (add-callback! evt cb)
  (set-event-callbacks! evt (cons cb (event-callbacks evt))))

(define (trigger! event [time #f])
  (when (and (not (triggered? event)) (not (completed? event)))
    (define sim (event-sim event))
    (set-event-scheduled! event (or time (sim-time sim)))
    (set-event-id! event ((sim-idgen sim)))
    (heap-add! (sim-queue sim) event)
    event))

(define (triggered? event)
  (if (event-scheduled event) #t #f))

(define completed? event-completed?)

(define (cancel-event! event)
  (when (and (triggered? event) (not (completed? event)))
    (define sim (event-sim event))
    (heap-remove! (sim-queue sim) event)))

(define (step! sim)
  (define queue (sim-queue sim))
  (when (> (heap-count queue) 0)
    (let* ([evt (heap-min queue)]
           [callbacks (event-callbacks evt)]
           [new-time (event-scheduled evt)])
      (heap-remove-min! queue)
      (when (not (event-completed? evt))
        (set-sim-time! sim new-time)
        (set-event-completed?! evt #t)
        (for ([cb callbacks])
          (cb))))))

(define (run! sim #:until [until #f])
  (let loop ()
    (cond
      [(= (heap-count (sim-queue sim)) 0) (void)]
      [(and (number? until) (>= (sim-time sim) until)) (void)]
      [else (begin
              (step! sim)
              (loop))])))

(define (wait sim delay #:value [val #f])
  (define cur-time (sim-time sim))
  (define evt (make-event sim #:value val))
  (trigger! evt (+ cur-time delay)))

(define (process sim fun)
  (define proc-evt (make-event sim))
  (define gen (fun sim))
  (define res #f)
  (define (cb)
    (set! res (if res (gen (event-value res)) (gen)))
    (if (event? res)
        (add-callback! res cb)
        (trigger! proc-evt)))
  (trigger! (make-event sim (list cb)))
  (void))

(struct resource (sim cap queue [users #:mutable]) #:transparent)
(struct event:request event (resource))

(define (make-resource sim cap)
  (resource sim cap (make-queue) '()))

(define (request res)
  (define sim (resource-sim res))
  (define put (event:request sim '() #f #f #f #f res))
  (if (< (length (resource-users res)) (resource-cap res))
      (begin
        (set-resource-users! res (cons put (resource-users res)))
        (trigger! put))
      (begin
        (enqueue! (resource-queue res) put)
        put)))

(define (release req)
  (define res (event:request-resource req))
  (define sim (resource-sim res))
  (if (memq req (resource-users res))
      (begin
        (set-resource-users! res (remq req (resource-users res) ))
        (when (not (queue-empty? (resource-queue res)))
          (define next (dequeue! (resource-queue res)))
          (set-resource-users! res (cons next (resource-users res)))
          (trigger! next)))
      (error  "Request is not in use by resource" req)))

(define (cancel-request! req)
  (define res (event:request-resource req))
  (queue-filter! (resource-queue res) (lambda (r)
                       (not (eq? r req)))))

(define-syntax (with-resource stx)
  (syntax-case stx ()
    [(_ (req exp) body ...) #'(let ([req exp])
                                body ...
                                (with-handlers
                                  ([exn:fail? (lambda (e) (cancel-request! req))])
                                  (release req)))]))


(define (any . evts)
  (define any-evt (make-event (event-sim (first evts)) #:value '()))
  (define ((cb evt))
    (set-event-value! any-evt (cons evt (event-value any-evt)))
    (trigger! any-evt))
  (for ([e evts])
    (add-callback! e (cb e)))
  any-evt)


(define mysim (make-sim))

(define ((cust name) sim)
  (generator ()
             (printf "~a entering @~a~%" name (sim-time sim))
             (with-resource [req (request counter)]
               (define result (yield (any req (wait sim 3))))
               (if (memq req result)
                   (begin 
                     (printf "~a at the counter @~a~%" name (sim-time sim))
                     (yield (wait sim 5))
                     (printf "~a is done @~a~%" name (sim-time sim)))
                   (printf "~a lost patience @~a~%" name (sim-time sim))))
             (printf "~a leaving @~a~%" name (sim-time sim))))

(define counter (make-resource mysim 1))

(define (source sim)
  (generator ()
             (process sim (cust "Adam"))
             (yield (wait sim 1))
             (process sim (cust "Bob"))
             (yield (wait sim 6))
             (process sim (cust "Charlie"))))

(process mysim source)
