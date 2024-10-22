insert_query = f"""
                        IF EXISTS (SELECT * FROM currency WHERE code='{row['code']}')
                        BEGIN
                            UPDATE currency
                            SET conv_value={row['value']}, LastUpdated='{row['LastUpdated']}'
                            WHERE code='{row['code']}'
                        END
                        ELSE BEGIN
                            INSERT INTO currency 
                            VALUES ('{row['code']}',{row['value']},'{row['LastUpdated']}')
                        END
                        """