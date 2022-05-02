import pandas as pd
import numpy as np
import random
import sys
import os
import uuid
import statsmodels.formula.api as sm
# %matplotlib notebook
# %matplotlib inline


def randid():
    return '%030x' % random.randrange(16**30)

def generate_correlated(r, Q):
    df = pd.DataFrame({
        'Q': Q, 
        'tmp': np.random.normal(0, 1, Q.shape[0])
    })
    lm = sm.ols(formula="tmp ~ Q", data=df).fit()
    generated = r * np.std(lm.resid) * Q + lm.resid * np.std(Q) * np.sqrt(1-r**2)
    return generated

# generate_correlated(0.76, np.random.normal(0, 1, 10))

def generate_candidate_table(df_gen_x, n_joinable_rows, rho=None):
    nrows = len(df_gen_x.index)
    
    if rho is None:
        C = np.random.normal(0, 1, nrows)
    else:
        C = generate_correlated(rho, df_gen_x.Q)
    
    df_gen_y = pd.DataFrame({'K': df_gen_x.K, 'C': C })
    
    # split in two parts: the joinable vs. not joinable
    df_gen_y_1 = df_gen_y.iloc[:n_joinable_rows].copy()
    df_gen_y_2 = df_gen_y.iloc[n_joinable_rows:].copy()

    # complete part 2 with new generated keys so it doesn't join
    df_gen_y_2['K'] = np.array([uuid.uuid1().hex
                                for i in range(nrows - n_joinable_rows)])

    return pd.concat([df_gen_y_1, df_gen_y_2])
    
def generate_queries_and_candidates(nrows, n_corr, n_uncorr):
    '''
    Generates one query table Q, and ncols candidate tables C that correlated
    and joinable with the query. The degree of joinability (JC similarity) and
    correlation level (rho) is drawn from a uniform distribution at random.
    '''
    K = [uuid.uuid1().hex for i in range(nrows)]
    Q = np.random.normal(0, 1, nrows)
    df_gen_x = pd.DataFrame({'K': K,'Q': Q})
    
    dfs = []
    for i in range(0, n_corr):
        rho_sign = np.random.randint(0, 2) * 2 - 1 # random +1 or -1
        rho = rho_sign * np.random.uniform(0.25, 1.0) # Pearson's Correlation
        jc  = np.random.uniform(0.1, 1.0) # Jaccard Containment
        n_joinable_rows = int(jc * nrows)
        dfs.append(generate_candidate_table(df_gen_x, n_joinable_rows, rho))

    for i in range(0, n_uncorr):
        jc  = np.random.uniform(0.1, 1.0) # Jaccard Containment
        n_joinable_rows = int(jc * nrows)
        dfs.append(generate_candidate_table(df_gen_x, n_joinable_rows, None))
        
    return df_gen_x, dfs

def create_files(basepath, n_queries, n_corr, n_uncorr, nrows=5000):
    if not os.path.isdir(basepath):
        os.makedirs(basepath)
        
    total = n_queries * (1 + n_corr + n_uncorr)
    count = 0
    
    for qid in range(n_queries):
       
        df_gen_query, gen_dfs = generate_queries_and_candidates(nrows, n_corr, n_uncorr)
        count += 1
        filename = 'synthetic-bivariate_qid={0:d}.csv'.format(qid)
        print('[{0:5.1f}%] Writing query table: {1}'.format(100*count//total, filename))
        df_gen_query.to_csv(os.path.join(basepath, filename), index=False)
            
        for cid in range(len(gen_dfs)):
            count += 1
            filename = 'synthetic-bivariate_qid={0:d}_cid={1:d}.csv'.format(qid, cid)
            print('[{0:5.1f}%] Writing candidate table: {1}'.format(100*count//total, filename))
            gen_dfs[cid].to_csv(os.path.join(basepath, filename), index=False)

def main():
    n = 300000
    print("Creating synthetic data...")
    create_files('./synthetic-table-corpus/', 1000, n_corr=100, n_uncorr=400, nrows=10000)
    print("done.")

if __name__ == "__main__":
    main()
