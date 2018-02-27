from mrjob.job import MRJob


class PopulationAreaStats(MRJob):

    def mapper(self, _, line):
        variables = line.split(',')

        state_name = variables[0]
        area = float(variables[3])
        population = float(variables[4])
        yield 'Area (Sq Miles)', ((area,state_name),(area,state_name),area,1)
        yield 'Population', ((population,state_name),(population,state_name),population,1)

    def combiner(self, key, values):
        count = 0
        current_min = float('Inf')
        min_state = None
        current_max = 0
        max_state = None
        current_sum = 0
        for v in values:
            if v[0][0] < current_min:
                current_min = v[0][0]
                min_state = v[0][1]
            if v[1][0] > current_max:
                current_max = v[1][0]
                max_state = v[1][1]
            current_sum += v[2]
            count += v[3]
        yield(key,((current_min,min_state),(current_max,max_state),current_sum,count))

    def reducer(self, key, values):
        count = 0
        current_min = float('Inf')
        min_state = None
        current_max = 0
        max_state = None
        current_sum = 0
        for v in values:
            if v[0][0] < current_min:
                current_min = v[0][0]
                min_state = v[0][1]
            if v[1][0] > current_max:
                current_max = v[1][0]
                max_state = v[1][1]
            current_sum += v[2]
            count += v[3]
        avg = current_sum/count
        print('----------{0}----------'.format(key))
        print('Min: {1} ({2})'.format(key,current_min,min_state))
        print('Max: {1} ({2})'.format(key,current_max,max_state))
        print('Mean: {1} '.format(key,avg))

        # yield(key,((current_min,min_state),(current_max,max_state),current_sum,count))


if __name__ == '__main__':
    PopulationAreaStats.run()
