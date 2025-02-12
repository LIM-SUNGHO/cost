import pandas as pd
import os
import logging

# 현재 스크립트 위치를 기준으로 작업 디렉토리 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

UPLOAD_DIR = "uploads"

# 로그 설정
logging.basicConfig(filename="log_BOM_추출_액상,추출액.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # 파일 경로 설정
        bom_file = os.path.join(UPLOAD_DIR,'BOM_추출_액상,추출액.csv')
        routing_file = os.path.join(UPLOAD_DIR,'raw_사전원가.csv')

        # 파일 불러오기
        bom_df = pd.read_csv(bom_file, encoding='utf-8-sig', low_memory=False)
        routing_df = pd.read_csv(routing_file, encoding='utf-8-sig', low_memory=False)
        logging.info("BOM 및 Routing 파일을 성공적으로 불러왔습니다.")

        # 데이터 타입 일치
        bom_df['품번'] = bom_df['품번'].astype(str)
        routing_df['품번'] = routing_df['품번'].astype(str)

        # 'raw_사전원가'에서 '원두투입'과 '추출량' 데이터 병합
        bom_df = pd.merge(bom_df, routing_df[['품번', '원두투입', '추출량']], on='품번', how='left')
        logging.info("품번 기준으로 '원두투입'과 '추출량' 데이터를 BOM에 병합 완료.")

        # 특정 조건에 따라 '추출량' 값 업데이트
        bom_df.loc[(bom_df['품번'] == '23421006') & (bom_df['자재번호'] == '40410001'), '추출량'] = 1018
        bom_df.loc[(bom_df['품번'] == '23421006') & (bom_df['자재번호'] == '40410002'), '추출량'] = 970

        logging.info("'품번'이 '23421006'이고 '자재번호'가 '40410001', '40410002'인 경우 '추출량' 값을 각각 1018, 970으로 업데이트 완료.")

        # '추출비용' 계산
        bom_df['추출비용'] = bom_df['단가_분쇄'] * bom_df['추출량비율'] * bom_df['원두투입'] / bom_df['추출량']
        logging.info("'추출비용' 계산 완료.")

        # '공정'이 '추출'인 값에서 '품번', '자재번호', '단가','비고','조달구분' 중복값을 제거
        bom_df = bom_df[~((bom_df['공정'] == '추출') & 
                          bom_df.duplicated(subset=['품번', '자재번호', '단가', '비고', '조달구분']))]
        logging.info("공정이 '추출'인 값에서 '품번', '자재번호', '단가', '비고', '조달구분' 중복값 제거 완료.")
        
        # '공정흐름차수명'이 '노무비', '제조경비', '0'인 데이터는 별도로 보존
        excluded_df = bom_df[(bom_df['품목대분류'] == '추출액') & bom_df['공정흐름차수명'].isin(['노무비', '제조경비', '0'])]

        # '품목대분류'가 '추출액'인 데이터에서 '공정흐름차수명' 제외 조건을 적용한 데이터 필터링
        filtered_df = bom_df[(bom_df['품목대분류'] == '추출액') & 
                            (~bom_df['공정흐름차수명'].isin(['노무비', '제조경비', '0']))]

        # 동일한 '품번' 내에서 '자재번호'가 다를 경우, 동일한 '자재번호' 값 중 가장 위 값만 보존
        deduplicated_df = filtered_df.sort_values(by=['품번', '자재번호']).drop_duplicates(subset=['품번', '자재번호'], keep='first')

        # '품목대분류'가 '추출액'이 아닌 데이터와 결합, 그리고 '공정흐름차수명' 보존 데이터 포함
        bom_df = pd.concat([bom_df[bom_df['품목대분류'] != '추출액'], deduplicated_df, excluded_df], ignore_index=True)

        logging.info("'품목대분류'가 '추출액'인 경우, '공정흐름차수명'이 '노무비', '제조경비', '0'은 보존하며, 나머지 데이터에 중복 제거를 적용 완료.")

        # 결과 저장
        bom_df.to_csv(bom_file, index=False, encoding='utf-8-sig')
        print("BOM_추출_액상,추출액에 '원두투입', '추출량', 및 '추출비용' 데이터 추가 및 갱신 완료 - BOM_추출_액상,추출액.csv 파일 저장")
        logging.info("BOM 추출 파일 갱신 작업이 완료되었습니다.")

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e}")
        print("오류: 파일을 찾을 수 없습니다.")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        print("오류 발생, 로그 파일을 확인하세요.")

if __name__ == "__main__":
    main()
