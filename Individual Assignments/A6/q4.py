from mongo.db_methods import report_reputation_summary, report_avg_questions_top10_tags, report_yearly_questions
import time

if __name__=="__main__":
    start_time = time.time()
    
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('A five-number summary (minimum, first quartile, median, second quartile, maximum) of the reputation of all users.')
    report_reputation_summary()
    print('Plot Saved')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('For the top 10 most used tags, the average number of questions for each tag as a bar chart.')
    report_avg_questions_top10_tags()
    print('Plot Saved')
    print('++++++++++++++++++++++++++++++++++++++++++++++')

    print('Number of questions asked each year as a time series plot.')
    report_yearly_questions()
    print('Plot Saved')
    print('++++++++++++++++++++++++++++++++++++++++++++++')


    end_time = time.time()
    run_time = end_time - start_time
    print("Total running time: ", run_time, " seconds")
