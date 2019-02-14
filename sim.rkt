#lang racket

(require racket/contract
         racket/generator
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
               [request (-> resource? event?)]
               [release (-> resource? event? any)])
 )

(struct sim ([time #:mutable] queue idgen) #:transparent)
(struct schedule-entry (time id event) #:transparent)

(define (make-sim)
  (define (event<=? x y)
    (and 
     (<= (schedule-entry-time x) (schedule-entry-time y))
     (<= (schedule-entry-id x) (schedule-entry-id y))))
  (define idgen (generator ()
                           (let loop ([id 0])
                             (yield id)
                             (loop (add1 id)))))
  (sim 0 (make-heap event<=?) idgen))

(struct event (sim [callbacks #:mutable] [completed? #:mutable]) #:transparent)

(define (make-event sim [callbacks '()])
  (event sim callbacks #f))

(define (add-callback! evt cb)
  (set-event-callbacks! evt (cons cb (event-callbacks evt))))

(define (trigger! event [time #f])
  (define sim (event-sim event))
  (set! time (or time (sim-time sim)))
  (heap-add! (sim-queue sim) (schedule-entry time ((sim-idgen sim)) event))
  event)

(define (step! sim)
  (define queue (sim-queue sim))
  (when (> (heap-count queue) 0)
    (let* ([entry (heap-min queue)]
           [evt (schedule-entry-event entry)]
           [callbacks (event-callbacks evt)]
           [new-time (schedule-entry-time entry)])
      (heap-remove-min! queue)
      (when (not (event-completed? evt))
        (set-sim-time! sim new-time)
        (for ([cb callbacks])
          (cb))
        (set-event-completed?! evt #t)))))

(define (run! sim #:until [until #f])
  (let loop ()
    (cond
      [(= (heap-count (sim-queue sim)) 0) (void)]
      [(and (number? until) (>= (sim-time sim) until)) (void)]
      [else (begin
              (step! sim)
              (loop))])))

(define (wait sim delay)
  (define cur-time (sim-time sim))
  (define evt (make-event sim))
  (trigger! evt (+ cur-time delay)))

(define (process sim fun)
  (define proc-evt (make-event sim))
  (define gen (fun sim))
  (define (cb)
    (define res (gen))
    (if (event? res)
        (add-callback! res cb)
        (trigger! proc-evt)))
  (trigger! (make-event sim (list cb)))
  proc-evt)

(struct resource (sim cap queue [users #:mutable]) #:transparent)

(define (make-resource sim cap)
  (resource sim cap (make-queue) '()))

(define (request res)
  (define sim (resource-sim res))
  (define put (make-event sim))
  (if (< (length (resource-users res)) (resource-cap res))
      (begin
        (set-resource-users! res (cons put (resource-users res)))
        (trigger! put))
      (begin
        (enqueue! (resource-queue res) put)
        put)))

(define (release res req)
  (define sim (resource-sim res))
  (if (memq req (resource-users res))
      (begin
        (set-resource-users! res (remq req (resource-users res) ))
        (when (not (queue-empty? (resource-queue res)))
          (define next (dequeue! (resource-queue res)))
          (set-resource-users! res (cons next (resource-users res)))
          (trigger! next)))
      (raise-argument-error 'request "Request is not in use by resource")))

(define (any . evts)
  (define any-evt (make-event (event-sim (first evts))))
  (define done? #f)
  (define (cb)
    (when (not done?)
      (set! done? #t)
      (trigger! any-evt)))
  (for ([e evts])
    (add-callback! e cb))
  any-evt)


(define mysim (make-sim))

(define ((cust name) sim)
  (generator ()
             (printf "~a entering @~a~%" name (sim-time sim))
             (define req (request counter))
             (yield req)
             (printf "~a at the counter @~a~%" name (sim-time sim))
             (yield (wait sim 5))
             (release counter req)
             (printf "~a leaving @~a~%" name (sim-time sim))))

(define counter (make-resource mysim 1))

(define adam (process mysim (cust "Adam")))
(define bob (process mysim (cust "Bob")))
