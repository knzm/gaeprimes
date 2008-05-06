import logging
import datetime

from google.appengine.api import users
from google.appengine.ext import webapp
from google.appengine.ext import db
from google.appengine.ext.webapp import template

from prime import Prime
import model


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
logging.basicConfig()


def retrieve_prime():
    log.debug("retrieve_prime()")
    primes = model.Prime.all().filter("owner =", None).order("number").fetch(1)
    if primes:
        prime = primes[0]
        current_user = users.get_current_user()
        prime.owner = current_user or "GUEST"
        prime.put()
    return prime


def assign_workspace():
    log.debug("assign_workspace()")
    workspaces = model.Workspace.all().filter("assigned_at =", None).fetch(1)
    if workspaces:
        log.debug("break-1")
        workspace = workspaces[0]
        workspace.assigned_at = datetime.datetime.now()
        workspace.put()
    elif not model.Workspace.all().fetch(1):
        sieves = model.Sieve.all().fetch(1)
        if sieves:
            log.debug("break-2")
            sieve = sieves[0]
            start = sieve.end
            end = start + chunk

            for cell in model.SieveCell.all().filter("sieve =", sieve):
                model.Prime(number=cell.number).put()
                cell.delete()
            sieve.delete()
        else:
            log.debug("break-3")
            for prime in Prime():
                if prime > chunk:
                    break
                model.Prime(number=prime).put()
            start = chunk
            end = start + chunk
        log.debug("break-4")

        sieve = model.Sieve(start=start, end=end)
        sieve.put()
        # log.debug("start=%d, end=%d", start, end)
        for number in xrange(start, end):
            # log.debug("number=%d", number)
            cell = model.SieveCell(number=number, sieve=sieve)
            cell.put()
        log.debug("break-5")
    return workspace


class Worker(object):
    def run(self):
        log.debug("Worker.run()")

        sieve = model.Sieve.all()
        sieve_item = sieve.filter("assigned_at =", None).fetch(1)

        sieve_item.assigned_at = datetime.datetime.now()
        sieve_item.put()

        n = int((sieve.start + modulo - 1) / modulo) * modulo
        if n == modulo:
            n += modulo
        while n < sieve.end:
            cells = model.SieveCell.all().filter("sieve =", sieve).filter("number =", n).fetch(1)
            if cells:
                cells[0].delete()
            n += modulo


class IndexPage(webapp.RequestHandler):
    def get(self):
        worker = Worker()
        worker.run()

        try:
            prime = retrieve_prime()
        except db.TransactionFailedError:
            prime = None

        self.response.out.write(
            template.render('index.html', {'prime': prime}))


def main():
    import wsgiref.handlers
    app = webapp.WSGIApplication(
        [('/', IndexPage)],
        debug=True)
    wsgiref.handlers.CGIHandler().run(app)


if __name__ == "__main__":
    main()
