from mrjob.job import MRJob


class VarElectricPrices(MRJob):

    def mapper(self, _, line):
        price = float(line.split(',')[1])

        yield 'Var(Electricity Prices)', price

    def reducer(self, key, values):
        price_sum = 0
        squared_price_sum = 0
        num_states = 0
        for v in values:
            price_sum += v
            squared_price_sum += v**2
            num_states += 1

        variance = squared_price_sum/num_states - (price_sum/num_states)**2
        yield key, variance

if __name__ == '__main__':
    VarElectricPrices.run()
