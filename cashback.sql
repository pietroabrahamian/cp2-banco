DECLARE
    CURSOR c_presentes IS
        SELECT
            i.ID          AS inscricao_id,
            i.USUARIO_ID,
            i.VALOR_PAGO,
            i.TIPO,
            u.NOME,
            u.SALDO,
            u.PRIORIDADE
        FROM INSCRICOES i
        JOIN USUARIOS u ON u.ID = i.USUARIO_ID
        WHERE i.STATUS = 'PRESENT';

    v_presencas    NUMBER;
    v_percentual   NUMBER;
    v_cashback     NUMBER;
    v_contador_ok  NUMBER := 0;
    v_contador_err NUMBER := 0;
    v_motivo       VARCHAR2(200);
    v_erro         VARCHAR2(500);

BEGIN
    FOR rec IN c_presentes LOOP
        BEGIN
            SELECT COUNT(*)
              INTO v_presencas
              FROM INSCRICOES
             WHERE USUARIO_ID = rec.USUARIO_ID
               AND STATUS = 'PRESENT';

            IF v_presencas > 3 THEN
                v_percentual := 0.25;
                v_motivo := 'Cashback 25pct - Ativista ' || v_presencas || ' presencas';
            ELSIF rec.TIPO = 'VIP' THEN
                v_percentual := 0.20;
                v_motivo := 'Cashback 20pct - VIP';
            ELSE
                v_percentual := 0.10;
                v_motivo := 'Cashback 10pct - Padrao';
            END IF;

            v_cashback := rec.VALOR_PAGO * v_percentual;

            UPDATE USUARIOS
               SET SALDO = SALDO + v_cashback
             WHERE ID = rec.USUARIO_ID;

            INSERT INTO LOG_AUDITORIA (INSCRICAO_ID, MOTIVO)
            VALUES (rec.inscricao_id,
                    v_motivo || ' | R$ ' || TO_CHAR(v_cashback, 'FM9999990.00'));

            v_contador_ok := v_contador_ok + 1;

        EXCEPTION
            WHEN OTHERS THEN
                v_contador_err := v_contador_err + 1;
                v_erro := SUBSTR(SQLERRM, 1, 490);
                INSERT INTO LOG_AUDITORIA (INSCRICAO_ID, MOTIVO)
                VALUES (rec.inscricao_id, v_erro);
        END;
    END LOOP;

    COMMIT;
    :out_ok  := v_contador_ok;
    :out_err := v_contador_err;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        :out_ok  := -1;
        :out_err := -1;
        RAISE;
END;